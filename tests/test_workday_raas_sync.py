import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import unittest
from functools import reduce

class WorkdayRaasSync(unittest.TestCase):
    def setUp(self):
        missing_envs = [x for x in [os.getenv('TAP_WORKDAY_RAAS_USERNAME'),
                                    os.getenv('TAP_WORKDAY_RAAS_PASSWORD')] if x == None]
        if len(missing_envs) != 0:
            raise Exception("set TAP_WORKDAY_RAAS_USERNAME, TAP_WORKDAY_RAAS_PASSWORD")

    def name(self):
        return "tap_tester_workday_raas_sync"

    def get_type(self):
        return "platform.workday-raas"

    def get_credentials(self):
        return {'password': os.getenv('TAP_WORKDAY_RAAS_PASSWORD')}

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
            'start_date' : '2015-03-15 00:00:00',
            'username': os.getenv('TAP_WORKDAY_RAAS_USERNAME'),
            'reports': '[{\"report_url\":\"https://i-0705abe4c72d24e6a.workdaysuv.com/ccx/service/customreport2/gms/lmcneil/stitch_test_report?End_Date=2020-02-19-08:00&Start_Date=2020-02-12-08:00\",\"report_name\":\"stitch_test_report\"}]'
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
            connections.select_catalog_via_metadata(conn_id, c, catalog_entry)

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
