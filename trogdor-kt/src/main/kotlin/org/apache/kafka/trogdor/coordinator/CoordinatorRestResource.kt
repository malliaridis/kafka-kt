/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.trogdor.coordinator

import java.util.concurrent.atomic.AtomicReference
import javax.servlet.ServletContext
import javax.ws.rs.Consumes
import javax.ws.rs.DELETE
import javax.ws.rs.DefaultValue
import javax.ws.rs.GET
import javax.ws.rs.NotFoundException
import javax.ws.rs.POST
import javax.ws.rs.PUT
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.core.Context
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response
import org.apache.kafka.trogdor.rest.CoordinatorShutdownRequest
import org.apache.kafka.trogdor.rest.CoordinatorStatusResponse
import org.apache.kafka.trogdor.rest.CreateTaskRequest
import org.apache.kafka.trogdor.rest.DestroyTaskRequest
import org.apache.kafka.trogdor.rest.Empty
import org.apache.kafka.trogdor.rest.StopTaskRequest
import org.apache.kafka.trogdor.rest.TaskRequest
import org.apache.kafka.trogdor.rest.TaskState
import org.apache.kafka.trogdor.rest.TaskStateType
import org.apache.kafka.trogdor.rest.TasksRequest
import org.apache.kafka.trogdor.rest.UptimeResponse

/**
 * The REST resource for the Coordinator. This describes the RPCs which the coordinator can accept.
 *
 * RPCs should be idempotent.  This is important because if the server's response is lost, the client will simply
 * retransmit the same request. The server's response must be the same the second time around.
 *
 * We return the empty JSON object {} rather than void for RPCs that have no results. This ensures that if we want
 * to add more return results later, we can do so in a compatible way.
 */
@Path("/coordinator")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
class CoordinatorRestResource {

    private val coordinator = AtomicReference<Coordinator>()

    @Context
    private var context: ServletContext? = null

    fun setCoordinator(myCoordinator: Coordinator) = coordinator.set(myCoordinator)

    @GET
    @Path("/status")
    @Throws(Throwable::class)
    fun status(): CoordinatorStatusResponse = coordinator().status()

    @GET
    @Path("/uptime")
    fun uptime(): UptimeResponse = coordinator().uptime()

    @POST
    @Path("/task/create")
    @Throws(Throwable::class)
    fun createTask(request: CreateTaskRequest): Empty {
        coordinator().createTask(request)
        return Empty.INSTANCE
    }

    @PUT
    @Path("/task/stop")
    @Throws(Throwable::class)
    fun stopTask(request: StopTaskRequest): Empty {
        coordinator().stopTask(request)
        return Empty.INSTANCE
    }

    @DELETE
    @Path("/tasks")
    @Throws(Throwable::class)
    fun destroyTask(@DefaultValue("") @QueryParam("taskId") taskId: String): Empty {
        coordinator().destroyTask(DestroyTaskRequest(taskId))
        return Empty.INSTANCE
    }

    @GET
    @Path("/tasks/")
    @Throws(Throwable::class)
    fun tasks(
        @QueryParam("taskId") taskId: List<String>?,
        @DefaultValue("0") @QueryParam("firstStartMs") firstStartMs: Long,
        @DefaultValue("0") @QueryParam("lastStartMs") lastStartMs: Long,
        @DefaultValue("0") @QueryParam("firstEndMs") firstEndMs: Long,
        @DefaultValue("0") @QueryParam("lastEndMs") lastEndMs: Long,
        @DefaultValue("") @QueryParam("state") state: String,
    ): Response {
        val isEmptyState = state == ""
        if (!isEmptyState && !TaskStateType.Constants.VALUES.contains(state)) {
            return Response.status(400).entity(
                "State $state is invalid. Must be one of ${TaskStateType.Constants.VALUES}"
            ).build()
        }
        val givenState = if (isEmptyState) null else TaskStateType.valueOf(state)
        val resp = coordinator().tasks(
            TasksRequest(
                taskIds = taskId,
                firstStartMs = firstStartMs,
                lastStartMs = lastStartMs,
                firstEndMs = firstEndMs,
                lastEndMs = lastEndMs,
                state = givenState,
            )
        )
        return Response.status(200).entity(resp).build()
    }

    @GET
    @Path("/tasks/{taskId}")
    @Throws(Throwable::class)
    fun tasks(@PathParam("taskId") taskId: String?): TaskState {
        return coordinator().task(TaskRequest(taskId))
            ?: throw NotFoundException("""No task with ID "$taskId" exists.""")
    }

    @PUT
    @Path("/shutdown")
    @Throws(Throwable::class)
    fun beginShutdown(request: CoordinatorShutdownRequest): Empty {
        coordinator().beginShutdown(request.stopAgents())
        return Empty.INSTANCE
    }

    private fun coordinator(): Coordinator {
        return coordinator.get() ?: throw RuntimeException("CoordinatorRestResource has not been initialized yet.")
    }
}
