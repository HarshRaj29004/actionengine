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

#include "actionengine/service/session.h"

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <absl/log/check.h>
#include <absl/log/log.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/time/clock.h>

#include "actionengine/actions/action.h"
#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/types.h"
#include "actionengine/net/stream.h"
#include "actionengine/nodes/async_node.h"
#include "actionengine/nodes/node_map.h"
#include "actionengine/stores/chunk_store.h"

namespace act {

ActionContext::~ActionContext() {
  act::MutexLock lock(&mu_);

  CancelContextInternal();
  WaitForActionsToDetachInternal();

  if (!running_actions_.empty()) {
    LOG(ERROR) << absl::StrFormat(
        "ActionContext::~ActionContext() timed out waiting for %v actions to "
        "detach. Please make sure that all actions react to cancellation and "
        "detach themselves from the context.",
        running_actions_.size());
  }

  // In debug builds, make it a fatal error if there are still running actions.
  DCHECK(running_actions_.empty());
}

absl::Status ActionContext::Dispatch(std::shared_ptr<Action> action) {
  act::MutexLock l(&mu_);
  if (cancelled_) {
    return absl::CancelledError("Action context is cancelled.");
  }

  if (running_actions_.contains(action.get())) {
    return absl::FailedPreconditionError(
        "Action is already running in this context.");
  }

  std::string action_id = action->GetId();
  if (actions_by_id_.contains(action_id)) {
    return absl::FailedPreconditionError(
        absl::StrCat("An action with ID ", action->GetId(),
                     " is already running in this context."));
  }

  Action* absl_nonnull action_ptr = action.get();
  running_actions_[action_ptr] =
      thread::NewTree({}, [action = std::move(action), this]() mutable {
        act::MutexLock lock(&mu_);

        mu_.unlock();
        if (const auto run_status = action->Run(); !run_status.ok()) {
          LOG(ERROR) << "Failed to run action: " << run_status;
        }
        mu_.lock();

        thread::Detach(ExtractActionFiber(action.get()));
        actions_by_id_.erase(action->GetId());
        cv_.SignalAll();
      });
  actions_by_id_[action_id] = action_ptr;

  return absl::OkStatus();
}

std::vector<std::shared_ptr<Action>> ActionContext::ListRunningActions() const {
  act::MutexLock lock(&mu_);
  std::vector<std::shared_ptr<Action>> actions;
  actions.reserve(running_actions_.size());
  for (const auto& [action_ptr, _] : running_actions_) {
    actions.push_back(action_ptr->shared_from_this());
  }
  return actions;
}

void ActionContext::CancelAction(Action* action) {
  act::MutexLock lock(&mu_);
  CancelActionInternal(action);
}

void ActionContext::CancelActionInternal(Action* absl_nonnull action)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (action == nullptr) {
    return;
  }

  const auto it = running_actions_.find(action);
  const auto id_it = actions_by_id_.find(action->GetId());

  if (it == running_actions_.end() && id_it != actions_by_id_.end()) {
    DLOG(FATAL) << absl::StrFormat(
        "Inconsistent state: action with ID %v found in actions_by_id_ but not "
        "in running_actions_.",
        action->GetId());
    ABSL_ASSUME(false);
  }

  if (id_it == actions_by_id_.end() && it != running_actions_.end()) {
    DLOG(FATAL) << absl::StrFormat(
        "Inconsistent state: action with ID %v found in running_actions_ but "
        "not in actions_by_id_.",
        action->GetId());
    ABSL_ASSUME(false);
  }

  action->Cancel();
  it->second->Cancel();
}

absl::Status ActionContext::CancelAction(const std::string_view action_id) {
  act::MutexLock lock(&mu_);
  const auto id_it = actions_by_id_.find(std::string(action_id));
  if (id_it == actions_by_id_.end()) {
    return absl::NotFoundError(
        absl::StrFormat("No action with ID %v found in context.", action_id));
  }
  CancelActionInternal(id_it->second);
  return absl::OkStatus();
}

void ActionContext::CancelContext() {
  act::MutexLock lock(&mu_);
  CancelContextInternal();
}

void ActionContext::WaitForActionsToDetach(absl::Duration cancel_timeout,
                                           absl::Duration detach_timeout) {
  act::MutexLock lock(&mu_);
  WaitForActionsToDetachInternal(cancel_timeout, detach_timeout);
}

void ActionContext::CancelContextInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (cancelled_) {
    return;
  }
  for (auto& [action, action_fiber] : running_actions_) {
    action->Cancel();
    action_fiber->Cancel();
  }
  cancelled_ = true;
  if (!running_actions_.empty()) {
    DLOG(INFO) << absl::StrFormat(
        "Action context cancelled, actions pending: %v.",
        running_actions_.size());
  }
}

std::unique_ptr<thread::Fiber> ActionContext::ExtractActionFiber(
    Action* action) {
  const auto map_node = running_actions_.extract(action);
  CHECK(!map_node.empty())
      << "Running action not found in session it was created in.";
  return std::move(map_node.mapped());
}

void ActionContext::WaitForActionsToDetachInternal(
    absl::Duration cancel_timeout, absl::Duration detach_timeout) {
  const absl::Time now = absl::Now();
  const absl::Time fiber_cancel_by = now + cancel_timeout;
  const absl::Time expect_actions_to_detach_by = now + detach_timeout;

  if (running_actions_.empty()) {
    return;
  }

  while (!running_actions_.empty()) {
    if (cv_.WaitWithDeadline(&mu_, fiber_cancel_by)) {
      break;
    }
  }
  if (running_actions_.empty()) {
    DLOG(INFO) << "All actions have detached cooperatively.";
  }

  CancelContextInternal();
  DLOG(INFO) << "Some actions are still running: sent cancellations and "
                "waiting for them to detach.";

  while (!running_actions_.empty()) {
    if (cv_.WaitWithDeadline(&mu_, expect_actions_to_detach_by)) {
      DLOG(ERROR) << "Timed out waiting for actions to detach.";
      break;
    }
  }
}

internal::ConnectionCtx::ConnectionCtx(Session* session,
                                       std::shared_ptr<WireStream> stream,
                                       StreamHandler handler,
                                       absl::Duration recv_timeout)
    : session_(session),
      stream_(std::move(stream)),
      status_(std::make_shared<absl::Status>(absl::StatusCode::kOk, "")) {
  DCHECK(stream_ != nullptr);
  fiber_ = thread::NewTree(
      {}, [status = status_, session = session_, stream_ = stream_,
           handler = std::move(handler), recv_timeout]() noexcept {
        *status = handler(stream_, session, recv_timeout);
      });
}

internal::ConnectionCtx::~ConnectionCtx() {
  if (fiber_ != nullptr) {
    fiber_->Cancel();
    fiber_->Join();
  }
  fiber_.reset();
  stream_.reset();
}

void internal::ConnectionCtx::CancelHandler() const {
  if (fiber_ == nullptr) {
    return;
  }
  fiber_->Cancel();
}

absl::Status internal::ConnectionCtx::Join() {
  if (fiber_ == nullptr) {
    return *status_;
  }
  fiber_->Join();
  fiber_.reset();
  stream_.reset();
  return *status_;
}

std::unique_ptr<thread::Fiber> internal::ConnectionCtx::ExtractHandlerFiber() {
  std::unique_ptr<thread::Fiber> fiber = std::move(fiber_);
  fiber_ = nullptr;
  return fiber;
}

bool IsReservedActionName(std::string_view name) {
  static auto* reserved_names = new absl::flat_hash_set<std::string_view>{
      "__ping", "__list_running_actions", "__cancel_action"};
  return reserved_names->contains(name);
}

StreamHandler internal::EnsureHalfClosesOrAbortsStream(StreamHandler handler) {
  return [handler = std::move(handler)](std::shared_ptr<WireStream> stream,
                                        Session* absl_nonnull session,
                                        absl::Duration recv_timeout) {
    absl::Status status;
    try {
      status = handler(stream, session, recv_timeout);
    } catch (const std::exception& e) {
      // Even though the whole Action Engine is noexcept, handlers might
      // not be: external library calls, rogue Python handlers, etc.
      status = absl::InternalError(
          absl::StrCat("Unhandled exception in stream handler: ", e.what()));
    }

    // If already half-closed or aborted (as should be by the handler),
    // these are no-ops.
    DCHECK(stream != nullptr);
    if (status.ok()) {
      stream->HalfClose();
    } else {
      stream->Abort(status);
    }

    return status;
  };
}

absl::Status internal::DefaultStreamHandler(std::shared_ptr<WireStream> stream,
                                            Session* session,
                                            absl::Duration recv_timeout) {
  if (stream == nullptr) {
    return absl::InvalidArgumentError("Stream cannot be null");
  }

  absl::Status status;
  while (!thread::Cancelled()) {
    absl::StatusOr<std::optional<WireMessage>> message =
        stream->Receive(recv_timeout);
    if (!message.ok()) {
      status = message.status();
      stream->Abort(status);
      break;
    }
    if (!message->has_value()) {
      break;
    }
    status =
        session->DispatchWireMessage(std::move(message)->value(), stream.get());
  }

  if (thread::Cancelled()) {
    status = absl::CancelledError("Service is shutting down.");
  }

  return status;
}

static absl::Status ReturnDispatchStatusReportingToCallerStream(
    WireStream* stream, const std::string& action_id, absl::Status status) {
  if (stream == nullptr) {
    return status;
  }
  std::vector fragments = {NodeFragment{
      .id = Action::MakeNodeId(action_id, "__dispatch_status__"),
      .data = ConvertTo<Chunk>(status).value(),
      .seq = 0,
      .continued = false,
  }};
  fragments.reserve(2);

  // If there was an error during dispatch, also report it on the __status__
  // because the caller may already not be listening to __dispatch_status__.
  if (!status.ok()) {
    fragments.push_back(NodeFragment{
        .id = Action::MakeNodeId(action_id, "__status__"),
        .data = ConvertTo<Chunk>(status).value(),
        .seq = 0,
        .continued = false,
    });
  }

  status.Update(
      stream->Send(WireMessage{.node_fragments = std::move(fragments)}));
  return status;
}

Session::~Session() {
  act::MutexLock lock(&mu_);

  // First, gracefully cancel all actions: they might still want to write
  // to nodes.
  action_context_.CancelContext();
  action_context_.WaitForActionsToDetach();

  // // Flush writers as our actions might have put data that has not yet
  // // been replicated to the streams.
  // if (node_map_ != nullptr) {
  //   node_map_->FlushAllWriters();
  // }

  for (auto& [_, connection] : connections_) {
    connection->CancelHandler();
  }
  for (auto& [_, connection] : connections_) {
    // release the lock because connection handlers may use action
    // registry, node map, etc.
    mu_.unlock();
    connection->Join().IgnoreError();
    mu_.lock();
  }
}

void Session::StartStreamHandler(std::string_view id,
                                 std::shared_ptr<WireStream> stream,
                                 StreamHandler handler,
                                 absl::Duration recv_timeout) {
  act::MutexLock lock(&mu_);
  connections_.emplace(
      id, std::make_unique<internal::ConnectionCtx>(
              this, std::move(stream),
              internal::EnsureHalfClosesOrAbortsStream(std::move(handler)),
              recv_timeout));
}

std::unique_ptr<internal::ConnectionCtx> Session::ExtractStreamHandler(
    std::string_view id) {
  act::MutexLock lock(&mu_);
  const auto map_node = connections_.extract(id);
  if (map_node.empty()) {
    return nullptr;
  }
  return std::move(map_node.mapped());
}

absl::flat_hash_map<std::string, std::unique_ptr<internal::ConnectionCtx>>
Session::ExtractAllStreamHandlers() {
  act::MutexLock lock(&mu_);
  auto extracted_connections = std::move(connections_);
  connections_.clear();
  return extracted_connections;
}

absl::Status Session::DispatchNodeFragment(NodeFragment node_fragment) {
  act::MutexLock lock(&mu_);
  return DispatchNodeFragmentInternal(std::move(node_fragment));
}

absl::Status Session::DispatchActionMessage(ActionMessage action_message,
                                            WireStream* origin_stream) {
  act::MutexLock lock(&mu_);
  return DispatchActionMessageInternal(std::move(action_message),
                                       origin_stream);
}

absl::Status Session::DispatchWireMessage(WireMessage message,
                                          WireStream* origin_stream) {
  act::MutexLock lock(&mu_);
  return DispatchWireMessageInternal(std::move(message), origin_stream);
}

size_t Session::GetNumActiveConnections() const {
  act::MutexLock lock(&mu_);
  return connections_.size();
}

AsyncNode* Session::GetNode(std::string_view id,
                            const ChunkStoreFactory& factory) const {
  act::MutexLock lock(&mu_);
  if (!node_map_) {
    return nullptr;
  }
  return node_map_->Get(id, factory);
}

NodeMap* Session::node_map() const {
  act::MutexLock lock(&mu_);
  return node_map_;
}

void Session::set_node_map(NodeMap* absl_nullable node_map) {
  act::MutexLock lock(&mu_);
  if (node_map_ != nullptr) {
    node_map_->FlushAllWriters();
  }
  node_map_ = node_map;
}

ActionRegistry* Session::action_registry() {
  act::MutexLock lock(&mu_);
  if (!action_registry_) {
    return nullptr;
  }
  return &*action_registry_;
}

void Session::set_action_registry(
    std::optional<ActionRegistry> action_registry) {
  act::MutexLock lock(&mu_);
  action_registry_ = std::move(action_registry);
}

absl::Status Session::DispatchNodeFragmentInternal(
    NodeFragment node_fragment) const {
  if (!node_map_) {
    return absl::FailedPreconditionError("Node map not set.");
  }
  return node_map_->Get(node_fragment.id)->Put(std::move(node_fragment));
}

absl::Status Session::DispatchReservedActionInternal(
    ActionMessage action_message, WireStream* origin_stream) {
  return absl::UnimplementedError(absl::StrCat(
      "Reserved actions are not implemented. Tried ", action_message.name));
}

absl::Status Session::DispatchActionMessageInternal(
    ActionMessage action_message, WireStream* origin_stream) {
  if (IsReservedActionName(action_message.name)) {
    // Reserved actions might use custom logic dealing with dispatch statuses,
    // thus raw return:
    return DispatchReservedActionInternal(std::move(action_message));
  }

  if (!action_registry_) {
    return ReturnDispatchStatusReportingToCallerStream(
        origin_stream, action_message.id,
        absl::FailedPreconditionError("Action registry not set."));
  }

  if (!action_registry_->IsRegistered(action_message.name)) {
    return ReturnDispatchStatusReportingToCallerStream(
        origin_stream, action_message.id,
        absl::NotFoundError(absl::StrCat(
            "Action not found in registry: ", action_message.name, ".")));
  }

  absl::StatusOr<std::unique_ptr<Action>> action_or_status =
      action_registry_->MakeAction(action_message.name, action_message.id,
                                   std::move(action_message.inputs),
                                   std::move(action_message.outputs));
  if (!action_or_status.ok()) {
    return ReturnDispatchStatusReportingToCallerStream(
        origin_stream, action_message.id, action_or_status.status());
  }

  std::unique_ptr<Action> action = *std::move(action_or_status);
  action->BindNodeMap(&*node_map_);
  action->BindSession(this);
  action->BindStream(origin_stream);

  // The session class is intended to represent a session where there is
  // another party involved. In this case, we want to clear inputs and outputs
  // after the action is run, because they will already have been sent to the
  // other party, and we don't want to keep them around locally.
  action->ClearInputsAfterRun(true);
  action->ClearOutputsAfterRun(true);

  return action_context_.Dispatch(std::move(action));
}

absl::Status Session::DispatchWireMessageInternal(WireMessage message,
                                                  WireStream* origin_stream) {

  std::vector<std::string> error_messages;

  for (auto& node_fragment : message.node_fragments) {
    std::string node_id = node_fragment.id;
    if (absl::Status status =
            DispatchNodeFragmentInternal(std::move(node_fragment));
        !status.ok()) {
      error_messages.push_back(
          absl::StrCat("node fragment ", node_id, ": ", status));
    }
  }

  for (auto& action_message : message.actions) {
    std::string action_name = action_message.name;
    if (absl::Status status = DispatchActionMessageInternal(
            std::move(action_message), origin_stream);
        !status.ok()) {
      error_messages.push_back(
          absl::StrCat("action ", action_name, ": ", status));
    }
  }

  if (!error_messages.empty()) {
    return absl::InvalidArgumentError(absl::StrJoin(error_messages, "\n"));
  }
  return absl::OkStatus();
}

}  // namespace act
