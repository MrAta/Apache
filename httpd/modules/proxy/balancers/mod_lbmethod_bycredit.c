/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "mod_proxy.h"
#include "scoreboard.h"
#include "ap_mpm.h"
#include "apr_version.h"
#include "ap_hooks.h"
#include <aws/core/Aws.h>
#include <aws/monitoring/CloudWatchClient.h>
#include <aws/monitoring/model/ListMetricsRequest.h>
#include <aws/monitoring/model/ListMetricsResult.h>
#include <iostream>
#include <iomanip>

module AP_MODULE_DECLARE_DATA lbmethod_bycredit_module;
static const char* SIMPLE_DATE_FORMAT_STR = "%Y-%m-%d";
static int worker_credit_balance(){
  Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
       Aws::CloudWatch::CloudWatchClient cw;
       Aws::CloudWatch::Model::ListMetricsRequest request;
      request.SetMetricName("CPUCreditBalance");


       bool done = false;
       bool header = false;

       while (!done)
       {
           auto outcome = cw.ListMetrics(request);
           if (!outcome.IsSuccess())
           {
               std::cout << "Failed to list cloudwatch metrics:" <<
                   outcome.GetError().GetMessage() << std::endl;
               break;
           }


           const auto &metrics = outcome.GetResult().GetMetrics();
           for (const auto &metric : metrics)
           {
               std::cout << std::left << std::setw(48) <<
                   metric.GetMetricName() << std::setw(32) <<
                   metric.GetNamespace();
               const auto &dimensions = metric.GetDimensions();
               for (auto iter = dimensions.cbegin();
                   iter != dimensions.cend(); ++iter)
               {
                   const auto &dimkv = *iter;
                   return dimkv.GetValue();
               }
           }

           const auto &next_token = outcome.GetResult().GetNextToken();
           request.SetNextToken(next_token);
           done = next_token.empty();
       }
   }
   Aws::ShutdownAPI(options);
   return 1;
}
static int (*ap_proxy_retry_worker_fn)(const char *proxy_function,
        proxy_worker *worker, server_rec *s) = NULL;

static proxy_worker *find_best_bycredit(proxy_balancer *balancer,
                                request_rec *r)
{

    int i;
    proxy_worker **worker;
    proxy_worker *mycandidate = NULL;
    int cur_lbset = 0;
    int max_lbset = 0;
    int checking_standby;
    int checked_standby;

    int total_factor = 0;

    if (!ap_proxy_retry_worker_fn) {
        ap_proxy_retry_worker_fn =
                APR_RETRIEVE_OPTIONAL_FN(ap_proxy_retry_worker);
        if (!ap_proxy_retry_worker_fn) {
            /* can only happen if mod_proxy isn't loaded */
            return NULL;
        }
    }

    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, r->server, APLOGNO(01211)
                 "proxy: Entering bycredit for BALANCER (%s)",
                 balancer->s->name);

    /* First try to see if we have available candidate */
    do {

        checking_standby = checked_standby = 0;
        while (!mycandidate && !checked_standby) {

            worker = (proxy_worker **)balancer->workers->elts;
            for (i = 0; i < balancer->workers->nelts; i++, worker++) {
                if  (!checking_standby) {    /* first time through */
                    if ((*worker)->s->lbset > max_lbset)
                        max_lbset = (*worker)->s->lbset;
                }
                if (
                    ((*worker)->s->lbset != cur_lbset) ||
                    (checking_standby ? !PROXY_WORKER_IS_STANDBY(*worker) : PROXY_WORKER_IS_STANDBY(*worker)) ||
                    (PROXY_WORKER_IS_DRAINING(*worker))
                    ) {
                    continue;
                }

                /* If the worker is in error state run
                 * retry on that worker. It will be marked as
                 * operational if the retry timeout is elapsed.
                 * The worker might still be unusable, but we try
                 * anyway.
                 */
                if (!PROXY_WORKER_IS_USABLE(*worker)) {
                    ap_proxy_retry_worker_fn("BALANCER", *worker, r->server);
                }

                /* Take into calculation only the workers that are
                 * not in error state or not disabled.
                 */
                if (PROXY_WORKER_IS_USABLE(*worker)) {

                    (*worker)->s->lbstatus += (*worker)->s->lbfactor;
                    total_factor += (*worker)->s->lbfactor;

                    if (!mycandidate
                        || worker_credit_balance((*worker)->s->) <=worker_credit_balance(mycandidate->s->busy) )
                        mycandidate = *worker;

                }

            }

            checked_standby = checking_standby++;

        }

        cur_lbset++;

    } while (cur_lbset <= max_lbset && !mycandidate);

    if (mycandidate) {
        mycandidate->s->lbstatus -= total_factor;
        ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, r->server, APLOGNO(01212)
                     "proxy: bycredit selected worker \"%s\" : busy %" APR_SIZE_T_FMT " : lbstatus %d",
                     mycandidate->s->name, mycandidate->s->busy, mycandidate->s->lbstatus);

    }

    return mycandidate;

}

/* assumed to be mutex protected by caller */
static apr_status_t reset(proxy_balancer *balancer, server_rec *s) {
    int i;
    proxy_worker **worker;
    worker = (proxy_worker **)balancer->workers->elts;
    for (i = 0; i < balancer->workers->nelts; i++, worker++) {
        (*worker)->s->lbstatus = 0;
        (*worker)->s->busy = 0;
    }
    return APR_SUCCESS;
}

static apr_status_t age(proxy_balancer *balancer, server_rec *s) {
        return APR_SUCCESS;
}

static const proxy_balancer_method bycredit =
{
    "bycredit",
    &find_best_bycredit,
    NULL,
    &reset,
    &age
};


static void register_hook(apr_pool_t *p)
{
    ap_register_provider(p, PROXY_LBMETHOD, "bycredit", "0", &bycredit);
}

AP_DECLARE_MODULE(lbmethod_bycredit) = {
    STANDARD20_MODULE_STUFF,
    NULL,       /* create per-directory config structure */
    NULL,       /* merge per-directory config structures */
    NULL,       /* create per-server config structure */
    NULL,       /* merge per-server config structures */
    NULL,       /* command apr_table_t */
    register_hook /* register hooks */
};
