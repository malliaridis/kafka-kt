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

package org.apache.kafka.trogdor.agent

import java.util.concurrent.atomic.AtomicReference
import javax.servlet.ServletContext
import javax.ws.rs.Consumes
import javax.ws.rs.DELETE
import javax.ws.rs.DefaultValue
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.PUT
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.core.Context
import javax.ws.rs.core.MediaType
import org.apache.kafka.trogdor.rest.AgentStatusResponse
import org.apache.kafka.trogdor.rest.CreateWorkerRequest
import org.apache.kafka.trogdor.rest.DestroyWorkerRequest
import org.apache.kafka.trogdor.rest.Empty
import org.apache.kafka.trogdor.rest.StopWorkerRequest
import org.apache.kafka.trogdor.rest.UptimeResponse

/**
 * The REST resource for the Agent. This describes the RPCs which the agent can accept.
 *
 * RPCs should be idempotent.  This is important because if the server's response is lost, the client will simply
 * retransmit the same request. The server's response must be the same the second time around.
 *
 * We return the empty JSON object {} rather than void for RPCs that have no results. This ensures that if we want
 * to add more return results later, we can do so in a compatible way.
 */
@Path("/agent")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
class AgentRestResource {

    private val agent: AtomicReference<Agent?> = AtomicReference(null)

    @Context
    private var context: ServletContext? = null

    fun setAgent(myAgent: Agent?) = agent.set(myAgent)

    @Throws(Throwable::class)
    @Path("/status")
    @GET
    fun getStatus(): AgentStatusResponse = agent().status()

    @GET
    @Path("/uptime")
    fun uptime(): UptimeResponse {
        return agent().uptime()
    }

    @POST
    @Path("/worker/create")
    @Throws(Throwable::class)
    fun createWorker(request: CreateWorkerRequest): Empty {
        agent().createWorker(request)
        return Empty.INSTANCE
    }

    @PUT
    @Path("/worker/stop")
    @Throws(Throwable::class)
    fun stopWorker(request: StopWorkerRequest): Empty {
        agent().stopWorker(request)
        return Empty.INSTANCE
    }

    @DELETE
    @Path("/worker")
    @Throws(Throwable::class)
    fun destroyWorker(@DefaultValue("0") @QueryParam("workerId") workerId: Long): Empty {
        agent().destroyWorker(DestroyWorkerRequest(workerId))
        return Empty.INSTANCE
    }

    @PUT
    @Path("/shutdown")
    @Throws(Throwable::class)
    fun shutdown(): Empty {
        agent().beginShutdown()
        return Empty.INSTANCE
    }

    private fun agent(): Agent =
        agent.get() ?: throw RuntimeException("AgentRestResource has not been initialized yet.")
}
