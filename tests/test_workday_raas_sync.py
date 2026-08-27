import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import unittest
from functools import reduce

import json

class WorkdayRaasSync(unittest.TestCase):
    def setUp(self):
        missing_envs = [x for x in [
            os.getenv('TAP_WORKDAY_RAAS_HOSTNAME'),
            os.getenv('TAP_WORKDAY_RAAS_TENANT'),
            os.getenv('TAP_WORKDAY_RAAS_CLIENT_ID'),
            os.getenv('TAP_WORKDAY_RAAS_CLIENT_SECRET'),
            os.getenv('TAP_WORKDAY_RAAS_REFRESH_TOKEN'),
        ] if x is None]
        if len(missing_envs) != 0:
            raise Exception(
                "set TAP_WORKDAY_RAAS_HOSTNAME, TAP_WORKDAY_RAAS_TENANT, "
                "TAP_WORKDAY_RAAS_CLIENT_ID, TAP_WORKDAY_RAAS_CLIENT_SECRET, "
                "TAP_WORKDAY_RAAS_REFRESH_TOKEN"
            )

    def name(self):
        return "tap_tester_workday_raas_sync"

    def get_type(self):
        return "platform.workday-raas"

    def get_credentials(self):
        return {
            'client_secret': os.getenv('TAP_WORKDAY_RAAS_CLIENT_SECRET'),
            'refresh_token': os.getenv('TAP_WORKDAY_RAAS_REFRESH_TOKEN'),
        }

    def expected_check_streams(self):
        return {'stitch_test_report'}

    def expected_sync_streams(self):
        return {'stitch_test_report'}

    def tap_name(self):
        return "tap-workday-raas"

    def expected_pks(self):
        return {'stitch_test_report': {}}

    def get_properties(self):
        return {
            'hostname': os.getenv('TAP_WORKDAY_RAAS_HOSTNAME'),
            'tenant': os.getenv('TAP_WORKDAY_RAAS_TENANT'),
            'client_id': os.getenv('TAP_WORKDAY_RAAS_CLIENT_ID'),
            'reports': json.dumps([{
                'report_url': 'https://{}/ccx/service/customreport2/{}/lmcneil/Stitch_Testing_2'.format(
                    os.getenv('TAP_WORKDAY_RAAS_HOSTNAME', ''),
                    os.getenv('TAP_WORKDAY_RAAS_TENANT', ''),
                ),
                'report_name': 'stitch_test_report',
            }]),
        }

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        #run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))

        diff = self.expected_check_streams().symmetric_difference( found_catalog_names )
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are kosher")

        # select all catalogs
        for c in found_catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, c['stream_id'])
            for field in catalog_entry['metadata']:
                field['metadata']['selected'] = True
            menagerie.write_metadata(conn_id, c['stream_id'], catalog_entry['metadata'])

        # clear state
        menagerie.set_state(conn_id, {})

        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # This should be validating the the PKs are written in each record

        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))
