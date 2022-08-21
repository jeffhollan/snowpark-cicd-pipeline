3 types of tests:
1. Unit tests: Test the logic of a specific part of the pipeline (one single operation set). Defined using a testing framework like pytest, using test environments.
2. Integration tests: Test the logic of a whole pipeline (multiple operations sets). Here I define the pipeline and a set of assertions to make on the output data - using test environments.
3. Data tests: Tests to run to ensure data quality. Here I'm going to define tests that run on a scheduled bases and perform a series of assertions on the production data.

