/*
 * Copyright 2021 The Kubeflow Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React from 'react';
import * as JsYaml from 'js-yaml';
import { useQuery } from 'react-query';
import { V2beta1Run } from 'src/apisv2beta1/run';
import { RouteParams } from 'src/components/Router';
import { Apis } from 'src/lib/Apis';
import { RunDetailsProps } from 'src/pages/RunDetails';
import { RunDetailsV2 } from 'src/pages/RunDetailsV2';

// KFP is V2-only: every run detail is rendered by the V2 page.
export default function RunDetailsRouter(props: RunDetailsProps) {
  const runId = props.match.params[RouteParams.runId];
  let pipelineManifest: string | undefined;

  // Retrieves v2 run detail.
  const {
    isLoading: isV2RunLoading,
    isSuccess: getV2RunSuccess,
    isError: isV2RunError,
    error: v2RunError,
    data: v2Run,
  } = useQuery<V2beta1Run, Error>(
    ['v2_run_detail', { id: runId }],
    () => Apis.runServiceApiV2.getRun(runId),
    {},
  );

  if (getV2RunSuccess && v2Run && v2Run.pipeline_spec) {
    pipelineManifest = JsYaml.safeDump(v2Run.pipeline_spec);
  }

  const pipelineId = v2Run?.pipeline_version_reference?.pipeline_id;
  const pipelineVersionId = v2Run?.pipeline_version_reference?.pipeline_version_id;

  const shouldLoadPipelineVersion =
    !pipelineManifest && !!pipelineId && !!pipelineVersionId;

  const {
    isLoading: isPipelineVersionLoading,
    isError: isPipelineVersionError,
    error: pipelineVersionError,
    data: templateStrFromPipelineVersion,
  } = useQuery<string, Error>(
    ['PipelineVersionTemplate', { pipelineId, pipelineVersionId }],
    async () => {
      if (!pipelineId || !pipelineVersionId) {
        return '';
      }
      const pipelineVersion = await Apis.pipelineServiceApiV2.getPipelineVersion(
        pipelineId,
        pipelineVersionId,
      );
      const pipelineSpec = pipelineVersion.pipeline_spec;
      return pipelineSpec ? JsYaml.safeDump(pipelineSpec) : '';
    },
    {
      enabled: shouldLoadPipelineVersion,
      staleTime: Infinity,
      cacheTime: Infinity,
    },
  );

  const templateString = pipelineManifest ?? templateStrFromPipelineVersion;

  if (isV2RunLoading || (shouldLoadPipelineVersion && isPipelineVersionLoading)) {
    return <div>Currently loading run information</div>;
  }

  if (isV2RunError) {
    return <div>Failed to load run: {v2RunError?.message}</div>;
  }

  if (isPipelineVersionError) {
    return <div>Failed to load pipeline version: {pipelineVersionError?.message}</div>;
  }

  if (getV2RunSuccess && v2Run && templateString) {
    return <RunDetailsV2 pipeline_job={templateString} run={v2Run} {...props} />;
  }

  if (getV2RunSuccess && v2Run) {
    return <div>Pipeline specification is not available for this run.</div>;
  }

  return <div>Currently loading run information</div>;
}
