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
  DLOG(INFO) << absl::StrFormat(
      "Action context cancelled, actions pending: %v.",
      running_actions_.size());
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

  while (!running_actions_.empty()) {
    if (cv_.WaitWithDeadline(&mu_, fiber_cancel_by)) {
      break;
    }
  }

  if (running_actions_.empty()) {
    DLOG(INFO) << "All actions have detached cooperatively.";
    return;
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

static ActionRegistry MakeSystemActionRegistry() {
  ActionRegistry registry;

  const ActionSchema& list_running_actions_schema =
      GetListRunningActionsSchema();
  registry.Register(list_running_actions_schema.name,
                    list_running_actions_schema, ListRunningActionsHandler);

  const ActionSchema& cancel_action_schema = GetCancelActionSchema();
  registry.Register(cancel_action_schema.name, cancel_action_schema,
                    CancelActionHandler);

  const ActionSchema& ping_action_schema = GetPingActionSchema();
  registry.Register(ping_action_schema.name, ping_action_schema,
                    PingActionHandler);

  return registry;
}

Session::Session(NodeMap* absl_nonnull node_map,
                 ActionRegistry* absl_nullable action_registry,
                 ChunkStoreFactory chunk_store_factory)
    : node_map_(node_map),
      action_registry_(action_registry),
      system_action_registry_(MakeSystemActionRegistry()),
      chunk_store_factory_(std::move(chunk_store_factory)),
      action_context_(std::make_unique<ActionContext>()) {}

Session::~Session() {
  act::MutexLock lock(&mu_);
  action_context_->CancelContext();
  action_context_->WaitForActionsToDetach();
  for (auto& [stream, _] : dispatch_tasks_) {
    // Abort each stream to cancel Receive in the dispatchers. If already
    // aborted or half-closed, this is a no-op.
    stream->Abort(absl::CancelledError("Session is being destroyed."));
  }
  JoinDispatchers(/*cancel=*/true);
}

AsyncNode* absl_nonnull Session::GetNode(
    const std::string_view id,
    const ChunkStoreFactory& chunk_store_factory) const {
  ChunkStoreFactory factory = chunk_store_factory;
  if (factory == nullptr) {
    factory = chunk_store_factory_;
  }
  return node_map_->Get(id, factory);
}

std::shared_ptr<AsyncNode> Session::BorrowNode(
    std::string_view id, const ChunkStoreFactory& chunk_store_factory) const {
  ChunkStoreFactory factory = chunk_store_factory;
  if (factory == nullptr) {
    factory = chunk_store_factory_;
  }
  return node_map_->Borrow(id, factory);
}

void Session::DispatchFrom(const std::shared_ptr<WireStream>& stream,
                           absl::AnyInvocable<void()> on_done) {
  act::MutexLock lock(&mu_);

  if (joined_) {
    return;
  }

  if (dispatch_tasks_.contains(stream.get())) {
    return;
  }

  dispatch_tasks_.emplace(
      stream.get(),
      thread::NewTree(
          {}, [this, stream, on_done = std::move(on_done)]() mutable {
            while (true) {
              absl::StatusOr<std::optional<WireMessage>> message =
                  stream->Receive(recv_timeout());
              if (!message.ok()) {
                DLOG(ERROR) << "Failed to receive message: " << message.status()
                            << " from stream: " << stream->GetId()
                            << ". Stopping dispatch.";
              }
              if (!message.ok()) {
                stream->Abort(message.status());
                break;
              }
              if (!message->has_value()) {
                stream->HalfClose();
                break;
              }
              if (absl::Status dispatch_status =
                      DispatchMessage(**std::move(message), stream.get());
                  !dispatch_status.ok()) {
                DLOG(ERROR) << "Failed to dispatch message: " << dispatch_status
                            << " from stream: " << stream->GetId()
                            << ". Stopping dispatch.";
                break;
              }
            }

            if (on_done) {
              std::move(on_done)();
            }

            std::unique_ptr<thread::Fiber> dispatcher_fiber;
            {
              act::MutexLock cleanup_lock(&mu_);
              if (const auto node = dispatch_tasks_.extract(stream.get());
                  !node.empty()) {
                dispatcher_fiber = std::move(node.mapped());
              }
            }
            if (dispatcher_fiber == nullptr) {
              return;
            }
            thread::Detach(std::move(dispatcher_fiber));
          }));
}

static absl::Status ReportDispatchStatusToCallerStream(
    WireStream* absl_nonnull stream, const std::string& action_id,
    const absl::Status& status) {
  std::vector fragments = {NodeFragment{
      .id = absl::StrCat(action_id, "#", "__dispatch_status__"),
      .data = ConvertTo<Chunk>(status).value(),
      .seq = 0,
      .continued = false,
  }};
  fragments.reserve(2);

  // If there was an error during dispatch, also report it on the __status__
  // because the caller may already not be listening to __dispatch_status__.
  if (!status.ok()) {
    fragments.push_back(NodeFragment{
        .id = absl::StrCat(action_id, "#", "__status__"),
        .data = ConvertTo<Chunk>(status).value(),
        .seq = 0,
        .continued = false,
    });
  }

  return stream->Send(WireMessage{.node_fragments = std::move(fragments)});
}

absl::Status Session::DispatchMessage(WireMessage message,
                                      WireStream* absl_nullable stream) {
  act::MutexLock lock(&mu_);
  if (joined_) {
    return absl::FailedPreconditionError(
        "Session has been joined, cannot dispatch messages.");
  }
  absl::Status status;

  std::vector<std::string> error_messages;

  for (auto& node_fragment : message.node_fragments) {
    const std::shared_ptr<AsyncNode> node = BorrowNode(node_fragment.id);
    const std::string node_id = node_fragment.id;
    if (absl::Status node_fragment_status = node->Put(std::move(node_fragment));
        !node_fragment_status.ok()) {
      error_messages.push_back(
          absl::StrCat("node fragment ", node_id, ": ", node_fragment_status));
      status.Update(node_fragment_status);
    }
  }

  for (auto& [action_id, action_name, inputs, outputs, headers] :
       message.actions) {

    if (!action_registry_->IsRegistered(action_name) &&
        !system_action_registry_.IsRegistered(action_name)) {
      std::string action_not_found_msg =
          absl::StrCat("Action not found: ", action_name);
      error_messages.push_back(action_not_found_msg);
      status.Update(absl::NotFoundError(action_not_found_msg));
      if (stream != nullptr) {
        ReportDispatchStatusToCallerStream(
            stream, action_id, absl::NotFoundError(action_not_found_msg))
            .IgnoreError();
      }
      continue;
    }

    absl::StatusOr<std::unique_ptr<Action>> action_or_status;
    if (system_action_registry_.IsRegistered(action_name) &&
        action_name != "__list_actions") {
      action_or_status = system_action_registry_.MakeAction(
          action_name, action_id, std::move(inputs), std::move(outputs));
    } else {
      action_or_status = action_registry_->MakeAction(
          action_name, action_id, std::move(inputs), std::move(outputs));
    }
    if (!action_or_status.ok()) {
      error_messages.push_back(absl::StrCat("action ", action_name, " ",
                                            action_id, ": ",
                                            action_or_status.status()));
      status.Update(action_or_status.status());
      if (stream != nullptr) {
        ReportDispatchStatusToCallerStream(stream, action_id,
                                           action_or_status.status())
            .IgnoreError();
      }
      continue;
    }

    std::unique_ptr<Action> action = std::move(*action_or_status);
    action->BindNodeMap(node_map_);
    action->BindSession(this);
    action->BindStream(stream);

    // The session class is intended to represent a session where there is
    // another party involved. In this case, we want to clear inputs and outputs
    // after the action is run, because they will already have been sent to the
    // other party, and we don't want to keep them around locally.
    action->ClearInputsAfterRun(true);
    action->ClearOutputsAfterRun(true);

    absl::Status action_dispatch_status =
        action_context_->Dispatch(std::move(action));
    if (!action_dispatch_status.ok()) {
      error_messages.push_back(absl::StrCat("action ", action_name, " ",
                                            action_id, ": ",
                                            action_dispatch_status));
      status.Update(action_dispatch_status);
      if (stream != nullptr) {
        ReportDispatchStatusToCallerStream(stream, action_id,
                                           action_dispatch_status)
            .IgnoreError();
      }
    }
  }

  if (!status.ok()) {
    DLOG(ERROR) << "Failed to dispatch message. Details:\n"
                << absl::StrJoin(error_messages, ";\n");
  }
  return status;
}

void Session::StopDispatchingFrom(WireStream* absl_nonnull stream) {
  std::unique_ptr<thread::Fiber> task;
  {
    act::MutexLock lock(&mu_);
    if (const auto node = dispatch_tasks_.extract(stream); !node.empty()) {
      task = std::move(node.mapped());
    }

    if (task == nullptr) {
      return;
    }
    task->Cancel();
  }

  task->Join();
}

void Session::StopDispatchingFromAll() {
  act::MutexLock lock(&mu_);
  JoinDispatchers(/*cancel=*/true);
}

void Session::JoinDispatchers(bool cancel) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  joined_ = true;

  std::vector<std::unique_ptr<thread::Fiber>> tasks_to_join;
  for (auto& [_, task] : dispatch_tasks_) {
    tasks_to_join.push_back(std::move(task));
  }

  if (cancel) {
    for (const auto& task : tasks_to_join) {
      task->Cancel();
    }
  }
  for (const auto& task : tasks_to_join) {
    mu_.unlock();
    task->Join();
    mu_.lock();
  }
}

const ActionSchema& GetListRunningActionsSchema() {
  static const ActionSchema* kListRunningActionsSchema = new ActionSchema{
      .name = "__list_running_actions",
      .inputs = {},
      .outputs = {{"action_ids", "text/plain"}},
  };
  return *kListRunningActionsSchema;
}

absl::Status ListRunningActionsHandler(const std::shared_ptr<Action>& action) {
  const Session* absl_nullable session = action->GetSession();
  if (session == nullptr) {
    return absl::FailedPreconditionError(
        "Cannot list actions: action is not bound to a session.");
  }
  std::vector<std::shared_ptr<Action>> running_actions =
      session->ListRunningActions();

  AsyncNode* output = action->GetOutput("action_ids");
  if (output == nullptr) {
    return absl::FailedPreconditionError(
        "Action has no 'action_ids' output. Cannot list running actions.");
  }

  for (const auto& running_action : running_actions) {
    RETURN_IF_ERROR(output->Put({
        .metadata = ChunkMetadata{.mimetype = "text/plain"},
        .data = running_action->GetId(),
    }));
  }

  return output->Put(EndOfStream());
}

const ActionSchema& GetCancelActionSchema() {
  static const ActionSchema* kCancelActionSchema = new ActionSchema{
      .name = "__cancel_action",
      .inputs = {{"action_id", "text/plain"}},
      .outputs = {},
  };
  return *kCancelActionSchema;
}

absl::Status CancelActionHandler(const std::shared_ptr<Action>& action) {
  ASSIGN_OR_RETURN(const auto action_id,
                   action->GetInput("action_id")->ConsumeAs<std::string>());
  const Session* absl_nullable session = action->GetSession();
  if (session == nullptr) {
    return absl::FailedPreconditionError(
        "Cannot cancel action: action is not bound to a session.");
  }
  return session->CancelAction(action_id);
}

const ActionSchema& GetPingActionSchema() {
  static const ActionSchema* kPingActionSchema = new ActionSchema{
      .name = "__ping",
      .inputs = {},
      .outputs = {},
  };
  return *kPingActionSchema;
}

absl::Status PingActionHandler(const std::shared_ptr<Action>& action) {
  return absl::OkStatus();
}

}  // namespace act
