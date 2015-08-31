/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <curl/curl.h>
#include <boost/regex.hpp>

#include <stout/lambda.hpp>
#include <stout/os.hpp>

#include <mesos/executor.hpp>

#include <assert.h>
#include <signal.h>

#include "rendler_helper.hpp"

using namespace mesos;

using std::cerr;
using std::cout;
using std::enable_shared_from_this;
using std::endl;
using std::lock_guard;
using std::make_pair;
using std::make_shared;
using std::mutex;
using std::shared_ptr;
using std::string;
using std::system_error;
using std::thread;
using std::unordered_map;
using std::weak_ptr;


class TaskObserver : public enable_shared_from_this<TaskObserver>
{
public:
  virtual void done(const TaskID &) = 0;

protected:
  virtual ~TaskObserver() {}
};


class RenderTask
{
public:
  typedef shared_ptr<RenderTask> pointer_type;

  RenderTask(
      ExecutorDriver *_driver,
      const TaskInfo &_task,
      shared_ptr<TaskObserver> _observer)
      : driver(_driver)
      , id(_task.task_id())
      , url(_task.data())
      , pid(-1)
      , observer(_observer)
  {
    assert(driver != nullptr);
    assert(!url.empty());
  }


  void run(const string &renderJSPath, const string &workDirPath)
  {
    printf("Running render task (%s): %s\n", id.value().c_str(), url.c_str());
    string outfile = workDirPath + id.value() + ".png";

    TaskStatus status;
    status.mutable_task_id()->MergeFrom(id);

    pid = fork();
    if (pid == 0) { // Child process.
      int returnCode =
        execl(
            "/usr/local/bin/phantomjs",
            "phantomjs",
            renderJSPath.c_str(),
            url.c_str(),
            outfile.c_str(),
            nullptr);

        if (-1 == returnCode) {
          fprintf(
              stderr,
              "Child %s failed to run phantomjs: %d\n",
              id.value().c_str(),
              errno);
          exit(errno);
        }
    } else if (pid > 0) { // Parent process.
      int exitStatus = 0;
      waitpid(pid, &exitStatus, 0);

      printf("Task %s %d\n", id.value().c_str(), exitStatus);

      if (0 != WIFEXITED(exitStatus) && 0 == WEXITSTATUS(exitStatus)) {
        printf("Task %s finished succesfuly.\n", id.value().c_str());
        status.set_state(TASK_FINISHED);
      } else if (WIFSIGNALED(exitStatus)) {
        printf("Task %s killed.\n", id.value().c_str());
        // We only send one signal, though it may not cover corner
        // cases like system shutting down.
        status.set_state(TASK_KILLED);
      } else {
        if (WIFEXITED(exitStatus)) {
          printf(
              "Task %s failed with exit code %d.\n",
              id.value().c_str(),
              WEXITSTATUS(exitStatus));
        } else {
          printf("Task %s failed for unknown reasons.\n", id.value().c_str());
        }
        status.set_state(TASK_FAILED);
      }
    } else { // Failed to fork.
      printf("Task %s failed to fork.\n", id.value().c_str());
      status.set_state(TASK_FAILED);
    }

    // Won't be called by child process, since it either gets its
    // image replaced by execl or exits the process.
    driver->sendStatusUpdate(status);

    auto _observer = observer.lock();

    if (_observer) {
      _observer->done(id);
    }
  }


  TaskID getId() const
  {
    return id;
  }


  void kill()
  {
    if (pid > 0) {
      ::kill(pid, SIGTERM);
    }
  }

private:
  ExecutorDriver *driver;
  TaskID id;
  string url;
  bool killed;
  pid_t pid;
  weak_ptr<TaskObserver> observer;
};


class RenderExecutor : public Executor, public TaskObserver
{
public:
  RenderExecutor(const string &_renderJSPath, const string &_workDirPath)
    : renderJSPath(_renderJSPath)
    , workDirPath(_workDirPath)
  {
  }

  virtual ~RenderExecutor()
  {
    for (auto &pair : tasks) {
      pair.second->kill();
    }
  }

  virtual void registered(ExecutorDriver* driver,
                          const ExecutorInfo& executorInfo,
                          const FrameworkInfo& frameworkInfo,
                          const SlaveInfo& slaveInfo)
  {
    cout << "Registered RenderExecutor on " << slaveInfo.hostname() << endl;
  }

  virtual void reregistered(ExecutorDriver* driver,
                            const SlaveInfo& slaveInfo)
  {
    cout << "Re-registered RenderExecutor on " << slaveInfo.hostname() << endl;
  }

  virtual void disconnected(ExecutorDriver* driver) {}


  virtual void launchTask(ExecutorDriver* driver, const TaskInfo& info)
  {
    cout << "Starting task " << info.task_id().value() << endl;

    shared_ptr<TaskObserver> self = shared_from_this();

    RenderTask::pointer_type task = make_shared<RenderTask>(driver, info, self);

    try {
      cout << "Trying to run task on its thread.\n";
      thread([=]() {task->run(workDirPath, workDirPath);}).detach();
    } catch (const system_error &err) {
      cerr << "Task launch failed: " << err.what() << '\n';
      TaskStatus status;
      status.mutable_task_id()->MergeFrom(info.task_id());
      status.set_state(TASK_FAILED);

      driver->sendStatusUpdate(status);

      return;
    }

    {
      lock_guard<mutex> lock(tasksMutex);
      tasks.insert(make_pair(task->getId().value(), task));
    }
  }


  virtual void killTask(ExecutorDriver* driver, const TaskID& taskId) {
      lock_guard<mutex> lock(tasksMutex);
      cout << "Got order to kill task " << taskId.value() << '\n';

      if (tasks.find(taskId.value()) != std::end(tasks)) {
        if (tasks[taskId.value()]) {
          tasks[taskId.value()]->kill();
        }
      } // what if else?
  }


  virtual void shutdown(ExecutorDriver* driver)
  {
    for (auto &pair : tasks) {
      pair.second->kill();
    }
  }


  virtual void frameworkMessage(ExecutorDriver* driver, const string& data) {}


  virtual void error(ExecutorDriver* driver, const string& message) {}


  void done(const TaskID &taskId) override {
    lock_guard<mutex> lock(tasksMutex);

    auto taskIter = tasks.find(taskId.value());

    if (taskIter != std::end(tasks)) {
      tasks.erase(taskIter);
    }
  }

private:
  mutex tasksMutex;
  unordered_map<string, RenderTask::pointer_type> tasks;
  string renderJSPath;
  string workDirPath;
};


int main(int argc, char** argv)
{
  string path = os::realpath(::dirname(argv[0])).get();
  string renderJSPath = path + "/render.js";
  string workDirPath = path + "/rendler-work-dir/";
  auto executor = make_shared<RenderExecutor>(renderJSPath, workDirPath);
  MesosExecutorDriver driver(executor.get());
  return driver.run() == DRIVER_STOPPED ? 0 : 1;
}
