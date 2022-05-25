import os
import sys
import json
import unittest
import warnings
import inspect

os.environ['LOG_LEVEL'] = "INFO"
import gvsu_housekeeping_orchestration


def ignore_warnings(test_func):
    def do_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            test_func(self, *args, **kwargs)
    return do_test

class TestHousekeepingOrchestration(unittest.TestCase):
    def __init__(self, name: str):
        super().__init__(name)
        sys.stderr = open(os.devnull, 'w')

    def __del__(self):
        sys.stderr.close()
        sys.stderr = sys.__stderr__

    def setUp(self):
        os.environ['EMR_CLUSTER_NAME_KEYWORD'] = "EMR-sunrise-test"
        os.environ['WAIT_MAX_ATTEMPTS'] = "4"

    def tearDown(self):
        pass
    
    @ignore_warnings
    def test01_add_hk_entry(self, assertEqual=None):
        print(f"\n********* {inspect.stack()[0][3]}")

        os.environ['ENVIRONMENT_ID'] = 'TEST'
        os.environ['AUTH_TOKEN'] = "true"

        os.environ['ROLE_ARN'] = "arn:aws:iam::123456789123:role/123456789123-Developer-Role"
        os.environ['AWS_LOCAL_RUNTIME'] = "true"
        os.environ['AWS_REGION'] = "us-east-1"

        os.environ['AWS_ACCESS_KEY_ID'] = "ACCESS_KEY_VALUE"
        os.environ['AWS_SECRET_ACCESS_KEY'] = "AWS_SECRET_ACCESS_KEY_VALUE"
        os.environ[
            'AWS_SESSION_TOKEN'] = "AWS_SESSION_TOKEN_VALUE"

        os.environ['AES_API_TIMEOUT'] = "60000"
        os.environ['EMR_CLUSTER_NAME_KEYWORD'] = "TEST"

        payload: str = """
        {
            "operation_id": 1,
            "run_instance": "20220516124416",
            "table_database": "jobsdaily_test",
            "table_name": "a218002_cc1g2msn0r7_20220516124416_atb_out_1",
            "hk_operation": "DELETE",
            "caller_id": "20220210666666_20220413140113_1640281166666",
            "notify_status_location": "s3://bucket-name/housekeeping",
            "job_database": "jobsdaily_test",
            "data_location": "s3://bucket-name/ClientName/AccountNumber/ProgramName/RUNID/table/20220204112104_1643752789355_1643756134126_OUTPUT",
            "request_ts": "2022-04-19 04:17:30.823",
            "tier": "Standard",
            "days_to_keep": "90",
            "ascend_tag": {
                "application": "GVAP",
                "account_number": "233140",
                "client": "Hegor",
                "project": "HK_Test",
                "team": "Sunrise"
            }
        }
        """
        event: dict = json.loads(payload)
        result: dict = gvsu_housekeeping_orchestration.lambda_handler(event, None)
        self.assertEqual(result['statusCode'], 200)
        return
