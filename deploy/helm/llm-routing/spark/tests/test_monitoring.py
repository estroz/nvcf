# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
import base64
import copy
import json
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, Mock, mock_open, patch

HERE = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import monitoring
import spark
try:
    import yaml
except ImportError:
    yaml = None


class MonitoringTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = json.loads((HERE/'config.example.json').read_text())
        self.recipe = spark.Recipe(self.config, self.tmp.name)
        self.recipe.bound_cluster = Mock()
        self.recipe.helm_apply = Mock()
        self.output = Mock(return_value='[]')
        self.monitor = monitoring.Monitoring(self.recipe, Mock(), self.output, spark.save)

    def test_old_and_explicitly_disabled_configs_do_not_enable_monitoring(self):
        for c in ({}, {'monitoring': {'enabled': False}}):
            self.assertFalse(monitoring.enabled(c))
        self.config['monitoring']['enabled'] = False
        self.assertNotIn('metrics', self.recipe.stack_values('a'*64, 'b'*64)['llm-api-gateway']['llmApiGateway'])
        with self.assertRaisesRegex(RuntimeError, 'enabled=true'):
            self.monitor.install()
        self.recipe.bound_cluster.assert_not_called()
        self.recipe.helm_apply.assert_not_called()

    def test_bad_config_does_not_silently_ignore_unknown_or_unsafe_values(self):
        for options in ({'enabled': 'true'}, {'enable': True}, {'images': {'grafana': 'grafana/grafana:latest'}},
                        {'namespaces': ['*']}, {'extraTargets': [{'name': 'pylon'}]}, {'imagePullPolicy': 'Sometimes'}):
            with self.subTest(options=options):
                self.config['monitoring'] = options
                with self.assertRaises(RuntimeError):
                    monitoring.chart_values(self.recipe)

    def test_actual_release_names_and_images_propagate(self):
        self.recipe.stack = 'custom-stack'
        self.recipe.operator = 'custom-operator'
        self.recipe.glm = 'custom-backend'
        self.config['monitoring'].update(images={'grafana': 'mirror.example/grafana:13.2.3'}, namespaces=[self.config['namespace'], 'other-models'])
        values = monitoring.chart_values(self.recipe)
        self.assertEqual(values['grafana']['image'], 'mirror.example/grafana:13.2.3')
        selectors = {t['name']: t['selector'] for t in values['targets']}
        self.assertIn('instance=custom-stack', selectors['gateway'])
        self.assertIn('instance=custom-operator', selectors['operator'])
        self.assertIn('instance=custom-backend', selectors['backend'])
        self.assertEqual(values['nodeSelector'], {'kubernetes.io/hostname': self.config['nodes']['control']})

    def test_network_policy_requires_explicit_api_hosts(self):
        for policy in ({'enabled': True}, {'enabled': 'true'}, {'apiServerCIDRs': ['0.0.0.0/0']},
                       {'apiServerCIDRs': ['not-an-ip/32']}, {'apiServerCIDRs': '10.0.0.1/32'}, {'unknown': True}):
            with self.subTest(policy=policy):
                self.config['monitoring']['networkPolicy'] = policy
                with self.assertRaises(RuntimeError):
                    monitoring.chart_values(self.recipe)
        policy = {'enabled': True, 'apiServerCIDRs': ['10.0.0.1/32', 'fd00::1/128']}
        self.config['monitoring']['networkPolicy'] = policy
        self.assertEqual(monitoring.chart_values(self.recipe)['networkPolicy'], policy)

    def test_install_only_changes_monitoring_and_preserves_model_state(self):
        self.recipe.state.update(serve=True, runtimeSha256='a'*64, attachedExisting=True)
        self.output.side_effect = ['[]', '']
        self.monitor.install()
        self.recipe.bound_cluster.assert_called_once()
        release, chart, values = self.recipe.helm_apply.call_args.args
        self.assertEqual(release, self.monitor.release)
        self.assertEqual(chart, monitoring.CHART)
        self.assertTrue(self.recipe.state['serve'])
        self.assertEqual(self.recipe.state['runtimeSha256'], 'a'*64)
        password = self.recipe.work/'grafana-admin-password'
        self.assertEqual(password.stat().st_mode & 0o777, 0o600)
        self.assertEqual(password.read_text().strip(), values['grafana']['adminPassword'])

    def test_reinstall_recovers_original_password_and_rejects_foreign_secrets(self):
        secret = {'metadata': {'annotations': {'meta.helm.sh/release-name': self.monitor.release,
                  'meta.helm.sh/release-namespace': self.config['namespace']}}, 'data': {'admin-password': base64.b64encode(b'original-password').decode()}}
        self.output.side_effect = [json.dumps([{'chart': 'llm-demo-monitoring-0.1.0'}]), json.dumps(secret)]
        self.monitor.install()
        self.assertEqual(self.recipe.helm_apply.call_args.args[2]['grafana']['adminPassword'], 'original-password')
        self.recipe.helm_apply.reset_mock()
        secret['metadata']['annotations']['meta.helm.sh/release-name'] = 'other'
        self.output.side_effect = ['[]', json.dumps(secret)]
        with self.assertRaisesRegex(RuntimeError, 'another installation'):
            self.monitor.install()
        self.recipe.helm_apply.assert_not_called()

    def test_missing_credentials_on_existing_release_fail_before_upgrade(self):
        self.output.side_effect = [json.dumps([{'chart': 'llm-demo-monitoring-0.1.0'}]), '']
        with self.assertRaisesRegex(RuntimeError, 'lost its credential'):
            self.monitor.install()
        self.recipe.helm_apply.assert_not_called()

    def test_failed_cluster_binding_prevents_writes(self):
        self.recipe.bound_cluster.side_effect = RuntimeError('Wrong cluster')
        with self.assertRaisesRegex(RuntimeError, 'Wrong cluster'):
            self.monitor.install()
        self.output.assert_not_called()
        self.recipe.helm_apply.assert_not_called()

    def test_metrics_verification_rejects_missing_failed_or_stale_components(self):
        result = {'status': 'success', 'data': {'result': [
            {'metric': {'component': 'gateway', 'pod': 'gateway-a'}, 'value': [0, '1']},
            {'metric': {'component': 'pylon', 'pod': 'pylon-a'}, 'value': [0, '1']}]}}
        self.assertTrue(monitoring.validate_scrapes(result, {'gateway', 'pylon'})['passed'])
        with self.assertRaisesRegex(RuntimeError, 'Missing or stale'):
            monitoring.validate_scrapes(result, {'gateway', 'operator'})
        result['data']['result'].append({'metric': {'component': 'pylon', 'pod': 'pylon-b'}, 'value': [0, '0']})
        with self.assertRaisesRegex(RuntimeError, 'Failed scrape'):
            monitoring.validate_scrapes(result, {'gateway', 'pylon'})
        with self.assertRaisesRegex(RuntimeError, 'query failed'):
            monitoring.validate_scrapes({'status': 'error'}, {'gateway'})

    def test_verifier_detects_missing_pylon_replica(self):
        result = {'status': 'success', 'data': {'result': [
            {'metric': {'component': 'pylon', 'namespace': 'models', 'pod': 'pylon-a'}, 'value': [0, '1']}]}}
        with self.assertRaisesRegex(RuntimeError, 'Running pods missing'):
            monitoring.validate_scrapes(result, {'pylon'}, {('pylon', 'models', 'pylon-a'), ('pylon', 'models', 'pylon-b')})

    def test_verifier_waits_for_initial_collection_before_checking_dashboard(self):
        import contextlib
        import io
        self.output.return_value = '{"items": []}'
        self.recipe.verify = Mock()
        (self.recipe.work/'grafana-admin-password').write_text('private-test-password')
        components = {'gateway', 'router', 'operator', 'pylon', 'backend',
                      'monitoring-storage', 'monitoring-grafana', 'monitoring-collector'}
        responses = [
            {'status': 'success', 'data': {'result': []}},
            {'status': 'success', 'data': {'result': [
                {'metric': {'component': component}, 'value': [0, '1']} for component in components]}},
            {'dashboard': {'uid': 'llm-demo', 'panels': [{'id': 1}]}}]
        with patch.object(self.monitor, 'forward', side_effect=lambda *args: contextlib.nullcontext()), \
             patch.object(monitoring.urllib.request, 'urlopen', side_effect=[io.StringIO(json.dumps(r)) for r in responses]) as request, \
             patch.object(monitoring.time, 'monotonic', side_effect=[0, 0]), \
             patch.object(monitoring.time, 'sleep') as sleep:
            self.monitor.verify(18000)

        self.assertEqual(request.call_count, 3)
        sleep.assert_called_once_with(2)
        self.recipe.verify.assert_not_called()
        report = json.loads((self.recipe.work/'evidence/monitoring.json').read_text())
        self.assertTrue(report['passed'])
        self.assertEqual(report['dashboardUid'], 'llm-demo')
        self.assertEqual({s['metric']['component'] for s in report['targets']}, components)

    def test_verifier_bounds_collection_wait_and_preserves_final_failure(self):
        import contextlib
        import io
        self.output.return_value = '{"items": []}'
        self.recipe.verify = Mock()
        components = {'gateway', 'router', 'operator', 'pylon', 'backend',
                      'monitoring-storage', 'monitoring-grafana', 'monitoring-collector'}
        missing = {'status': 'success', 'data': {'result': []}}
        failed = {'status': 'success', 'data': {'result': [
            {'metric': {'component': component}, 'value': [0, '0' if component == 'gateway' else '1']}
            for component in components]}}
        with patch.object(self.monitor, 'forward', side_effect=lambda *args: contextlib.nullcontext()) as forward, \
             patch.object(monitoring.urllib.request, 'urlopen', side_effect=[io.StringIO(json.dumps(r)) for r in (missing, missing, failed)]) as request, \
             patch.object(monitoring.time, 'monotonic', side_effect=[0, 0, 74, 75]), \
             patch.object(monitoring.time, 'sleep') as sleep:
            with self.assertRaisesRegex(RuntimeError, 'Failed scrape targets:.*gateway'):
                self.monitor.verify(18000, traffic=True)

        self.assertEqual(request.call_count, 3)
        self.assertEqual([call.args for call in sleep.call_args_list], [(2,), (1,)])
        forward.assert_called_once_with('victoria-metrics', 18000, 8428)
        self.recipe.verify.assert_not_called()
        report = json.loads((self.recipe.work/'evidence/monitoring.json').read_text())
        self.assertFalse(report['passed'])
        self.assertIn('startedAt', report)

    def test_image_export_checks_archive_architecture_with_both_docker_clis(self):
        import io
        import tarfile
        archive = self.recipe.work/'metrics.tar'
        images = monitoring.image_list(self.recipe)
        for platform_flag in (True, False):
            for architecture in ('arm64', 'amd64'):
                with self.subTest(platform_flag=platform_flag, architecture=architecture):
                    self.monitor.run.reset_mock()
                    self.output.return_value = '--platform' if platform_flag else 'Usage: docker save'
                    with tarfile.open(archive, 'w') as tar:
                        for name, data in {'manifest.json': [{'Config': 'image.json', 'RepoTags': images}],
                                           'image.json': {'architecture': architecture, 'os': 'linux'}}.items():
                            raw = json.dumps(data).encode()
                            info = tarfile.TarInfo(name)
                            info.size = len(raw)
                            tar.addfile(info, io.BytesIO(raw))
                    if architecture == 'arm64':
                        self.monitor.export_images(archive)
                    else:
                        with self.assertRaisesRegex(RuntimeError, 'non-ARM64'):
                            self.monitor.export_images(archive)
                    calls = [call.args[0] for call in self.monitor.run.call_args_list]
                    self.assertEqual(calls[:-1], [['docker','pull','--platform','linux/arm64',image] for image in images])
                    command = ['docker','save'] + (['--platform','linux/arm64'] if platform_flag else [])
                    self.assertEqual(calls[-1], command+['-o',archive]+images)

    def test_monitoring_import_validates_images_and_targets_only_control_node(self):
        import io
        import tarfile
        archive = self.recipe.work/'images.tar'
        manifest = json.dumps([{'RepoTags': monitoring.image_list(self.recipe)}]).encode()
        with tarfile.open(archive, 'w') as tar:
            info = tarfile.TarInfo('manifest.json')
            info.size = len(manifest)
            tar.addfile(info, io.BytesIO(manifest))
        # Stop at the first apply so the test cannot operate a runtime socket.
        self.recipe.helm_apply.side_effect = RuntimeError('captured import')
        with patch.object(spark, 'output', side_effect=['[]', '{"items": []}']), self.assertRaisesRegex(RuntimeError, 'captured import'):
            self.recipe.import_images(archive, True, monitoring_only=True)
        values = self.recipe.helm_apply.call_args.args[2]
        self.assertEqual(values['nodeNames'], [self.config['nodes']['control']])
        self.assertEqual(values['archiveSizeLimit'], '2Gi')
        self.assertEqual(len(values['archiveSha256']), 64)

    def test_remote_upload_accepts_monitoring_size_and_rejects_excess_or_corruption(self):
        import ast
        tree = ast.parse((HERE/'spark.py').read_text())
        upload = next(node.value.value for node in ast.walk(tree) if isinstance(node, ast.Assign)
                      and any(isinstance(t, ast.Name) and t.id == 'upload' for t in node.targets))
        archive_size = 1536 * 1024**2
        for received, digest, error in [(archive_size, 'valid', None),
                                        (archive_size + 1, 'valid', 'exceeds'),
                                        (archive_size - 1, 'valid', 'differs'),
                                        (archive_size, 'corrupt', 'differs')]:
            with self.subTest(received=received, digest=digest):
                chunk = MagicMock()
                chunk.__len__.return_value = received
                stdin = Mock()
                stdin.buffer.read.side_effect = [chunk, b'']
                checksum = Mock()
                checksum.hexdigest.return_value = digest
                with patch('sys.argv', ['upload', '/images/test.tar', 'valid', str(archive_size)]), \
                     patch('sys.stdin', stdin), patch('builtins.open', mock_open()), \
                     patch('hashlib.sha256', return_value=checksum), patch('os.chmod'), \
                     patch('os.path.exists', return_value=True), patch('os.unlink') as unlink, \
                     patch('os.replace') as replace:
                    if error:
                        with self.assertRaisesRegex(RuntimeError, error):
                            exec(upload, {})
                        replace.assert_not_called()
                    else:
                        exec(upload, {})
                        replace.assert_called_once_with('/images/test.tar.upload', '/images/test.tar')
                    unlink.assert_called_once_with('/images/test.tar.upload')

    def test_dashboard_reports_a_lost_tunnel(self):
        import contextlib
        proc = Mock()
        proc.poll.return_value = 1
        with patch.object(self.monitor, 'forward', return_value=contextlib.nullcontext(proc)):
            with self.assertRaisesRegex(RuntimeError, 'Grafana tunnel disconnected'):
                self.monitor.dashboard(13000)

    def test_traffic_verification_requires_latency_and_both_token_types_in_each_mode(self):
        import contextlib
        import io
        metrics = {
            'requests': ('llm_api_gateway_http_requests_total',),
            'durationCount': ('llm_api_gateway_http_request_duration_seconds_count',),
            'durationSeconds': ('llm_api_gateway_http_request_duration_seconds_sum',),
            'firstToken': ('llm_api_gateway_stream_first_token_seconds_count',),
            'firstTokenSeconds': ('llm_api_gateway_stream_first_token_seconds_sum',),
            'streamPromptTokens': ('llm_api_gateway_llm_tokens_total', 'token_type="prompt"', 'stream="true"'),
            'nonstreamPromptTokens': ('llm_api_gateway_llm_tokens_total', 'token_type="prompt"', 'stream="false"'),
            'streamTokens': ('llm_api_gateway_llm_tokens_total', 'token_type="completion"', 'stream="true"'),
            'nonstreamTokens': ('llm_api_gateway_llm_tokens_total', 'token_type="completion"', 'stream="false"')}
        for stalled_metric in (None, *metrics):
            with self.subTest(stalled_metric=stalled_metric):
                self.output.return_value = '{"items": []}'
                self.output.side_effect = None
                (self.recipe.work/'grafana-admin-password').write_text('private-test-password')
                traffic_sent = False
                def send_traffic(*args):
                    nonlocal traffic_sent
                    traffic_sent = True
                self.recipe.verify = Mock(side_effect=send_traffic)
                components = {'gateway','router','operator','pylon','backend','monitoring-storage','monitoring-grafana','monitoring-collector'}
                def response(request, timeout):
                    url = request if isinstance(request, str) else request.full_url
                    if '/api/dashboards/' in url:
                        return io.StringIO(json.dumps({'dashboard': {'uid': 'llm-demo', 'panels': [{'id': 1}]}}))
                    query = monitoring.urllib.parse.parse_qs(monitoring.urllib.parse.urlparse(url).query)['query'][0]
                    if query.startswith('up{'):
                        data = [{'metric': {'component': c}, 'value': [0,'1']} for c in components]
                    else:
                        self.assertIn('monitoring_release="'+self.monitor.release+'"', query)
                        self.assertIn('model="GLM-5.3-UD-IQ2_M"', query)
                        stalled = stalled_metric and all(part in query for part in metrics[stalled_metric])
                        increased = traffic_sent and not stalled
                        data = [{'value': [0, '2' if increased else '1']}]
                    return io.StringIO(json.dumps({'status': 'success','data': {'result': data}}))
                with patch.object(self.monitor, 'forward', side_effect=lambda *args: contextlib.nullcontext()), \
                     patch.object(monitoring.urllib.request, 'urlopen', side_effect=response), \
                     patch.object(monitoring.time, 'monotonic', side_effect=[0, 0, 100]):
                    if stalled_metric is None:
                        self.monitor.verify(18000, traffic=True)
                    else:
                        with self.assertRaisesRegex(RuntimeError, 'did not increase'):
                            self.monitor.verify(18000, traffic=True)
                report = json.loads((self.recipe.work/'evidence/monitoring.json').read_text())
                self.assertEqual(report['passed'], stalled_metric is None)
                if stalled_metric is None:
                    self.assertEqual(report['traffic']['before'], dict.fromkeys(metrics, 1))
                    self.assertEqual(report['traffic']['after'], dict.fromkeys(metrics, 2))
                    self.assertEqual(report['dashboardUid'], 'llm-demo')
                self.assertNotIn('private-test-password', json.dumps(report))
                self.recipe.verify.assert_called_once_with(True, 18001)

    def test_enabled_stack_installs_monitoring_after_routing(self):
        calls = []
        self.recipe.helm_apply.side_effect = lambda release, *args: calls.append(release)
        responses = [json.dumps({'data': {'cluster-token': base64.b64encode(b'worker').decode()}}),
                     json.dumps({'data': {'ca.crt': 'CA'}})]
        with patch.object(self.recipe, 'prepare'), patch.object(spark, 'output', side_effect=responses), \
             patch.object(monitoring.Monitoring, 'install', side_effect=lambda: calls.append('monitoring')):
            self.recipe.deploy_stack()
        self.assertEqual(calls, [self.recipe.operator, self.recipe.stack, 'monitoring'])


@unittest.skipUnless(yaml and shutil.which('helm'), 'Install requirements-monitoring.txt and Helm for chart tests')
class MonitoringChartTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.tmp.cleanup)
        cls.recipe = spark.Recipe(json.loads((HERE/'config.example.json').read_text()), cls.tmp.name)
        cls.recipe.render()
        cls.docs = [d for d in yaml.safe_load_all((cls.recipe.work/'render/monitoring.yaml').read_text()) if d]
        cls.config = yaml.safe_load(next(d for d in cls.docs if d['kind']=='ConfigMap' and d['metadata']['name'].endswith('-collector'))['data']['config.yaml'])

    def test_disabled_chart_creates_no_resources(self):
        result = subprocess.check_output(['helm', 'template', 'disabled', str(monitoring.CHART)], text=True)
        self.assertFalse([doc for doc in yaml.safe_load_all(result) if doc])

    def test_optional_egress_policy_selects_only_monitoring_and_allows_cluster_access(self):
        self.assertFalse(any(d['kind']=='NetworkPolicy' for d in self.docs))
        values = monitoring.chart_values(self.recipe)
        values['networkPolicy'] = {'enabled': True, 'apiServerCIDRs': ['10.0.0.1/32']}
        path = self.recipe.work/'restricted.json'
        path.write_text(json.dumps(values))
        rendered = subprocess.check_output(['helm', 'template', 'restricted', str(monitoring.CHART), '-f', str(path)], text=True)
        policy = next(d for d in yaml.safe_load_all(rendered) if d and d['kind']=='NetworkPolicy')['spec']
        self.assertEqual(policy['podSelector'], {'matchLabels': {'app.kubernetes.io/instance': 'restricted'}})
        self.assertEqual(policy['policyTypes'], ['Egress'])
        self.assertEqual(policy['egress'], [{'to': [{'namespaceSelector': {}}]},
                         {'to': [{'ipBlock': {'cidr': '10.0.0.1/32'}}],
                          'ports': [{'protocol': 'TCP', 'port': 443}, {'protocol': 'TCP', 'port': 6443}]}])

    def test_scrapes_select_actual_rendered_workloads_once(self):
        values = monitoring.chart_values(self.recipe)
        workloads = []
        for name in ('stack', 'operator', 'glm-serve'):
            workloads.extend(d for d in yaml.safe_load_all((self.recipe.work/'render'/(name+'.yaml')).read_text()) if d and d['kind']=='Deployment')
        for t in values['targets']:
            if t['name']=='pylon':
                continue
            labels = dict(pair.split('=', 1) for pair in t['selector'].split(','))
            selected = [d for d in workloads if all(d['spec']['template']['metadata']['labels'].get(k)==v for k,v in labels.items())]
            self.assertEqual(len(selected), 1, t)
            ports = [p for c in selected[0]['spec']['template']['spec']['containers'] for p in c.get('ports', []) if p['name']==t['portName']]
            self.assertEqual(len(ports), 1, t)

    def test_collection_has_scoped_read_only_rbac_and_no_cluster_role(self):
        self.assertFalse(any(d['kind'].startswith('ClusterRole') for d in self.docs))
        roles = [d for d in self.docs if d['kind']=='Role']
        self.assertEqual([d['metadata']['namespace'] for d in roles], [self.recipe.c['namespace']])
        self.assertEqual(roles[0]['rules'], [{'apiGroups': [''], 'resources': ['pods'], 'verbs': ['get','list','watch']}])

    def test_metrics_pipeline_and_discovery_include_all_components(self):
        config = self.config
        pipeline = config['service']['pipelines']['metrics']
        self.assertEqual(pipeline['exporters'], ['prometheusremotewrite'])
        jobs = config['receivers']['prometheus']['config']['scrape_configs']
        self.assertEqual({j['job_name'] for j in jobs}, {'gateway','router','operator','pylon','backend','monitoring-storage','monitoring-grafana','monitoring-collector'})
        for job in jobs[:5]:
            self.assertEqual(job['kubernetes_sd_configs'][0]['namespaces']['names'], [self.recipe.c['namespace']])
            self.assertEqual(job['relabel_configs'][1]['action'], 'keep')
        self.assertEqual(jobs[0]['relabel_configs'][2]['replacement'], '$${1}:9464')

    def test_workloads_have_limits_control_placement_and_private_access(self):
        for doc in self.docs:
            if doc['kind']=='Deployment':
                pod = doc['spec']['template']['spec']
                self.assertEqual(pod['nodeSelector']['kubernetes.io/hostname'], self.recipe.c['nodes']['control'])
                self.assertTrue(pod['securityContext']['runAsNonRoot'])
                self.assertTrue(pod['containers'][0]['resources']['limits']['memory'])
            if doc['kind']=='Service':
                self.assertEqual(doc['spec']['type'], 'ClusterIP')
        pvc = next(d for d in self.docs if d['kind']=='PersistentVolumeClaim')
        self.assertEqual(pvc['metadata']['annotations']['helm.sh/resource-policy'], 'keep')
        grafana = next(d for d in self.docs if d['kind']=='Deployment' and d['metadata']['name'].endswith('-grafana'))
        env = {e['name']: e for e in grafana['spec']['template']['spec']['containers'][0]['env']}
        self.assertEqual(env['GF_AUTH_ANONYMOUS_ENABLED']['value'], 'false')
        self.assertIn('secretKeyRef', env['GF_SECURITY_ADMIN_PASSWORD']['valueFrom'])

    def test_dashboard_provisioning_and_queries_cover_required_signals(self):
        config = next(d for d in self.docs if d['kind']=='ConfigMap' and d['metadata']['name'].endswith('-grafana'))['data']
        datasource = yaml.safe_load(config['datasource.yaml'])['datasources'][0]
        dashboard = json.loads(config['dashboard.json'])
        self.assertEqual(datasource['uid'], 'demo-metrics')
        self.assertEqual(dashboard['uid'], 'llm-demo')
        expressions = ' '.join(t['expr'] for p in dashboard['panels'] for t in p['targets'])
        for metric in ('stream_first_token_seconds_bucket','llm_tokens_total','pylon_reverse_tunnel_connected','nvcf_pylon_operator_registered','stargate_requests_total','requests_deferred'):
            self.assertIn(metric, expressions)
        self.assertNotIn('or vector(0)', expressions)
        self.assertIn('timestamp(up', expressions)
        for panel in dashboard['panels']:
            self.assertEqual(panel['datasource']['uid'], datasource['uid'])
            self.assertEqual(panel['fieldConfig']['defaults']['noValue'], 'No data')


if __name__ == '__main__':
    unittest.main()
