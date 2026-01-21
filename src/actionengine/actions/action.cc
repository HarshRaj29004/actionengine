// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "actionengine/actions/action.h"

#include <functional>

#include <absl/base/nullability.h>
#include <absl/base/thread_annotations.h>
#include <absl/log/check.h>
#include <absl/log/log.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/time/clock.h>

#include "actionengine/actions/registry.h"
#include "actionengine/actions/schema.h"
#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/conversion.h"
#include "actionengine/data/types.h"
#include "actionengine/net/stream.h"
#include "actionengine/nodes/async_node.h"
#include "actionengine/nodes/node_map.h"
#include "actionengine/service/session.h"
#include "actionengine/stores/chunk_store_reader.h"
#include "actionengine/stores/chunk_store_writer.h"
#include "actionengine/util/random.h"
#include "actionengine/util/status_macros.h"

namespace act {

Action::Action(ActionSchema schema, std::string_view id,
               std::vector<Port> inputs, std::vector<Port> outputs)
    : schema_(std::move(schema)),
      id_(id.empty() ? GenerateUUID4() : std::string(id)),
      cancelled_(std::make_unique<thread::PermanentEvent>()) {
  absl::StatusOr<ActionMessage> message_or = schema_.GetActionMessage(id_);
  DCHECK_OK(message_or.status())
      << "Invariant violated: getting action message from a schema should only "
         "NOT succeed if the id is empty.";
  ActionMessage& message = *message_or;

  std::vector<Port>& input_parameters =
      inputs.empty() ? message.inputs : inputs;
  for (auto& [input_name, input_id] : std::move(input_parameters)) {
    input_name_to_id_[std::move(input_name)] = std::move(input_id);
  }

  std::vector<Port>& output_parameters =
      outputs.empty() ? message.outputs : outputs;
  for (auto& [output_name, output_id] : std::move(output_parameters)) {
    output_name_to_id_[std::move(output_name)] = std::move(output_id);
  }
}

Action::~Action() {
  act::MutexLock lock(&mu_);

  reffed_readers_.clear();

  if (has_been_run_ && !run_status_.has_value()) {
    CancelInternal();
    const absl::Time deadline = absl::Now() + absl::Seconds(10);
    while (!run_status_.has_value()) {
      if (cv_.WaitWithDeadline(&mu_, deadline)) {
        LOG(WARNING) << absl::StrFormat(
            "Action destructor timed out waiting for action %s (id=%s) to "
            "complete after cancellation.",
            schema_.name, id_);
        break;
      }
    }
  }
}

std::string Action::MakeNodeId(std::string_view action_id,
                               std::string_view node_name) {
  return absl::StrCat(action_id, "#", node_name);
}

ActionMessage Action::GetActionMessage() const {
  std::vector<Port> input_parameters;
  input_parameters.reserve(input_name_to_id_.size());
  for (const auto& [name, id] : input_name_to_id_) {
    input_parameters.push_back({
        .name = name,
        .id = id,
    });
  }

  std::vector<Port> output_parameters;
  output_parameters.reserve(output_name_to_id_.size());
  for (const auto& [name, id] : output_name_to_id_) {
    output_parameters.push_back({
        .name = name,
        .id = id,
    });
  }

  act::MutexLock lock(&mu_);
  return {
      .id = id_,
      .name = schema_.name,
      .inputs = input_parameters,
      .outputs = output_parameters,
      .headers = headers_,
  };
}

AsyncNode* absl_nullable Action::GetNode(std::string_view id) {
  if (input_name_to_id_.contains(id)) {
    return GetInput(id);
  }
  if (output_name_to_id_.contains(id)) {
    return GetOutput(id);
  }

  return nullptr;
}

AsyncNode* absl_nullable Action::GetInput(std::string_view name,
                                          std::optional<bool> bind_stream) {
  act::MutexLock lock(&mu_);
  if (!input_name_to_id_.contains(name)) {
    return nullptr;
  }
  const auto it = borrowed_inputs_.find(name);
  if (it == borrowed_inputs_.end()) {
    return nullptr;
  }
  AsyncNode* node = it->second.get();
  if (stream_ != nullptr &&
      bind_stream.value_or(bind_streams_on_inputs_default_)) {
    absl::flat_hash_map<std::string, WireStream*> peers;
    peers.insert({std::string(stream_->GetId()), stream_});
    node->GetWriter().BindPeers(std::move(peers));
    nodes_with_bound_streams_.insert(node);
  }

  ChunkStoreReader* reader = &node->GetReader();

  // Keep track of all readers we have bound to this action, so that we can
  // cancel them when the action is done to avoid frozen chunk store readers
  // on input nodes where no inputs are sent, or insufficient inputs are sent.
  if (!reffed_readers_.contains(reader)) {
    reffed_readers_.insert(reader);
  }

  if (cancelled_->HasBeenNotified()) {
    reader->Cancel();
  }

  return node;
}

void Action::BindNodeMap(NodeMap* absl_nullable node_map) {
  act::MutexLock lock(&mu_);
  borrowed_inputs_.clear();
  borrowed_outputs_.clear();

  if (node_map == nullptr) {
    node_map_ = nullptr;
    return;
  }

  node_map_ = node_map;
  for (const auto& [input_name, input_id] : input_name_to_id_) {
    borrowed_inputs_[input_name] = node_map_->Borrow(input_id);
  }
  for (const auto& [output_name, output_id] : output_name_to_id_) {
    borrowed_outputs_[output_name] = node_map_->Borrow(output_id);
  }
  borrowed_outputs_["__status__"] =
      node_map_->Borrow(GetOutputId("__status__"));
  borrowed_outputs_["__dispatch_status__"] =
      node_map_->Borrow(GetOutputId("__dispatch_status__"));
}

NodeMap* absl_nullable Action::GetNodeMap() const {
  act::MutexLock lock(&mu_);
  return node_map_;
}

void Action::BindStream(WireStream* absl_nullable stream) {
  act::MutexLock lock(&mu_);
  stream_ = stream;
}

WireStream* absl_nullable Action::GetStream() const {
  act::MutexLock lock(&mu_);
  return stream_;
}

void Action::BindSession(Session* absl_nullable session) {
  act::MutexLock lock(&mu_);
  session_ = session;
}

Session* absl_nullable Action::GetSession() const {
  act::MutexLock lock(&mu_);
  return session_;
}

void Action::BindRegistry(ActionRegistry* registry) {
  act::MutexLock lock(&mu_);
  registry_ = registry;
}

absl::Status Action::Await(absl::Duration timeout) {
  const absl::Time started_at = absl::Now();

  act::MutexLock lock(&mu_);
  if (has_been_run_) {
    while (!run_status_) {
      if (cv_.WaitWithDeadline(&mu_, started_at + timeout) && !run_status_) {
        return absl::DeadlineExceededError(
            absl::StrFormat("Awaiting action %s (id=%s) timed out after %v",
                            schema_.name, id_, timeout));
      }
    }
    return *run_status_;
  }

  if (has_been_called_) {
    AsyncNode* status_node =
        GetOutputInternal("__status__", /*bind_stream=*/false);

    mu_.unlock();
    absl::StatusOr<absl::Status> run_status =
        status_node->ConsumeAs<absl::Status>(timeout);
    mu_.lock();

    if (!run_status.ok()) {
      LOG(ERROR) << absl::StrFormat("Failed to consume status from node %s: %s",
                                    status_node->GetId(),
                                    run_status.status().message());
      return run_status.status();
    }
    return *run_status;
  }

  return absl::FailedPreconditionError(
      "Action has not been run or called yet. Awaiting is only possible "
      "after Run() or Call() has been invoked.");
}

absl::Status Action::Call(
    absl::flat_hash_map<std::string, std::string> wire_message_headers) {
  act::MutexLock lock(&mu_);
  bind_streams_on_inputs_default_ = true;
  bind_streams_on_outputs_default_ = false;
  has_been_called_ = true;

  if (WireStream* stream = stream_; stream != nullptr) {
    mu_.unlock();
    absl::Status status =
        stream->Send(WireMessage{.actions = {GetActionMessage()},
                                 .headers = std::move(wire_message_headers)});
    mu_.lock();
    return status;
  }

  return absl::OkStatus();
}

absl::StatusOr<absl::Status> Action::CallAndWaitForDispatchStatus(
    absl::flat_hash_map<std::string, std::string> wire_message_headers) {
  RETURN_IF_ERROR(Call(std::move(wire_message_headers)));
  act::MutexLock lock(&mu_);
  AsyncNode* dispatch_status_node =
      GetOutputInternal("__dispatch_status__", /*bind_stream=*/false);

  mu_.unlock();
  absl::StatusOr<absl::Status> dispatch_status =
      dispatch_status_node->ConsumeAs<absl::Status>();
  mu_.lock();

  return dispatch_status;
}

absl::Status Action::Run() {
  act::MutexLock lock(&mu_);
  bind_streams_on_inputs_default_ = false;
  bind_streams_on_outputs_default_ = true;

  if (has_been_run_) {
    return absl::FailedPreconditionError(
        absl::StrFormat("Action %s with id=%s has already been run. "
                        "Cannot run the action again.",
                        schema_.name, id_));
  }

  if (handler_ == nullptr) {
    return absl::FailedPreconditionError(
        absl::StrFormat("Action %s with id=%s has no handler bound. "
                        "Cannot run the action.",
                        schema_.name, id_));
  }

  has_been_run_ = true;

  mu_.unlock();
  absl::Status handler_status = handler_(shared_from_this());
  mu_.lock();

  ASSIGN_OR_RETURN(Chunk handler_status_chunk,
                   ConvertTo<Chunk>(handler_status));

  // Propagate error statuses to all output nodes.
  if (!handler_status.ok()) {
    for (const auto& [output_name, output_node] : borrowed_outputs_) {
      if (output_name == "__status__" || output_name == "__dispatch_status__") {
        continue;
      }
      output_node->Put(handler_status_chunk, /*seq=*/-1, /*final=*/true)
          .IgnoreError();
    }
  }

  // Wait for all output nodes to finish writing.
  for (const auto& [output_name, output_node] : borrowed_outputs_) {
    if (output_name == "__status__") {
      continue;
    }
    output_node->GetWriter().WaitForBufferToDrain();
  }

  for (auto& reader : reffed_readers_) {
    reader->Cancel();
  }
  reffed_readers_.clear();

  UnbindStreams();

  absl::Status full_run_status = handler_status;
  if (stream_ != nullptr) {
    full_run_status.Update(
        stream_->Send(WireMessage{.node_fragments = {NodeFragment{
                                      .id = GetOutputId("__status__"),
                                      .data = handler_status_chunk,
                                      .seq = 0,
                                      .continued = false,
                                  }}}));
  }
  stream_ = nullptr;

  AsyncNode* status_node =
      GetOutputInternal("__status__", /*bind_stream=*/false);
  full_run_status.Update(status_node->Put(handler_status_chunk,
                                          /*seq=*/0, /*final=*/true));
  status_node->GetWriter().WaitForBufferToDrain();

  run_status_ = full_run_status;
  cv_.SignalAll();

  if ((!clear_inputs_after_run_ && !clear_outputs_after_run_) ||
      node_map_ == nullptr) {
    // If no clearing is requested, we can return early.
    run_status_ = full_run_status;
    cv_.SignalAll();
    return full_run_status;
  }

  if (clear_inputs_after_run_) {
    for (const auto& [input_name, input_id] : input_name_to_id_) {
      node_map_->Extract(GetInputId(input_name)).reset();
    }
  }

  if (clear_outputs_after_run_) {
    for (const auto& [output_name, output_id] : output_name_to_id_) {
      node_map_->Extract(GetOutputId(output_name)).reset();
    }
  }

  run_status_ = full_run_status;
  cv_.SignalAll();
  return full_run_status;
}

void Action::ClearInputsAfterRun(bool clear) {
  act::MutexLock lock(&mu_);
  clear_inputs_after_run_ = clear;
}

void Action::ClearOutputsAfterRun(bool clear) {
  act::MutexLock lock(&mu_);
  clear_outputs_after_run_ = clear;
}

void Action::CancelInternal() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (cancelled_->HasBeenNotified() || run_status_) {
    return;
  }

  for (const auto& [input_name, input_node] : borrowed_inputs_) {
    input_node->GetReader().Cancel();
  }

  cancelled_->Notify();
}

bool Action::Cancelled() const {
  act::MutexLock lock(&mu_);
  return cancelled_->HasBeenNotified();
}

std::string Action::GetInputId(const std::string_view name) const {
  return Action::MakeNodeId(id_, name);
}

std::string Action::GetOutputId(const std::string_view name) const {
  return Action::MakeNodeId(id_, name);
}

AsyncNode* absl_nullable Action::GetOutputInternal(
    std::string_view name, const std::optional<bool> bind_stream) {
  const auto it = borrowed_outputs_.find(name);
  if (it == borrowed_outputs_.end()) {
    return nullptr;
  }
  AsyncNode* node = it->second.get();

  if (stream_ != nullptr &&
      bind_stream.value_or(bind_streams_on_outputs_default_) &&
      name != "__status__" && name != "__dispatch_status__") {
    absl::flat_hash_map<std::string, WireStream*> peers;
    peers.insert({std::string(stream_->GetId()), stream_});
    node->GetWriter().BindPeers(std::move(peers));
    nodes_with_bound_streams_.insert(node);
  }
  return node;
}

void Action::UnbindStreams() {
  for (const auto& node : nodes_with_bound_streams_) {
    if (node == nullptr) {
      continue;
    }
    ChunkStoreWriter& writer = node->GetWriter();
    writer.BindPeers({});
  }
  nodes_with_bound_streams_.clear();
}

ActionRegistry* absl_nullable Action::GetRegistry() const {
  act::MutexLock lock(&mu_);
  return registry_ != nullptr
             ? registry_
             : (session_ != nullptr ? session_->action_registry() : nullptr);
}

absl::StatusOr<std::unique_ptr<Action>> Action::MakeActionInSameSession(
    const std::string_view name, const std::string_view action_id) const {
  ActionRegistry* absl_nullable registry = GetRegistry();
  if (registry == nullptr) {
    return nullptr;
  }
  if (!registry->IsRegistered(name)) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto action, registry->MakeAction(name, action_id));
  act::MutexLock lock(&mu_);
  action->BindNodeMap(node_map_);
  action->BindStream(stream_);
  action->BindSession(session_);
  return action;
}

}  // namespace act