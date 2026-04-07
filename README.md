# route_planner_demo

# run tests
python3 -m venv venv
source venv/bin/activate
pip install pytest boto3

pytest test_handler.py -v

