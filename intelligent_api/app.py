from flask import Flask, request, jsonify
import json


from dotenv import load_dotenv
import os

# load .env
load_dotenv()

# retrieve env variables
qwen_api_key = os.getenv('QWEN_API_KEY')
qwen_openai_sdk_base_url = os.getenv('QWEN_OPENAI_SDK_BASE_URL')


app = Flask(__name__)

def extract_data_lineage_from_spark_sql(SQL: str, llm_model: str = 'qwen2.5-72b-instruct'):
    import re
    from openai import OpenAI
    
    msg = f"""Spark SQL Code: ```{SQL}```
    requirements:
    1. Give the result in the Json format.
        the json contains 'spark_data_lineage`, which is a list containing
        `target_result`: targe result table name or file name,
        `source_data`: the list of table names or source file names used to get the target result , 
        `transformation`: the calculation steps show how `target_result` is calculated from the `source_data`. 
                                The step should be a description sentence including operation/transformation and the dataframe/table/view involved
                                take care of the column match.
    2. Consider the input data from DB or files as `source_data`. consider the calcaluted data that is shown to user or saved as the `target_result`.
    3. The intermediate view or table or dataframe cannot be views as `source_data`.
    4. Give the json result only.
    """
    try:
        client = OpenAI(
            api_key=qwen_api_key, 
            base_url=qwen_openai_sdk_base_url,
        )
        completion = client.chat.completions.create(
            model=llm_model,
            messages=[
                {'role': 'system', 'content': 'You are a Spark SQL expert. Given a Spark SQL, please help user to identify the source data and target result and analyze the transformation from source data to targe result'},
                {'role': 'user', 'content': msg}],
            temperature = 1,
            )
        pattern = r'```json(.*?)```'

        matches = re.findall(pattern, completion.choices[0].message.content, re.DOTALL)
        return {"result": matches[0]}
    except:
        return {"result": "```json\n\n```"}

def compare_spark_code_difference(original_sql_code: str, revised_sql_code: str, llm_model: str = 'qwen2.5-72b-instruct'):
    # Placeholder for the actual logic to compare Spark SQL code differences
    # For demonstration, we'll just return a simple dictionary
    import re
    from openai import OpenAI
    
    msg = f"""
    Original Spark SQL Code: ```{original_sql_code}```
    Revised Spark SQL Code: ```{revised_sql_code}```

    requirements:
    1. Give the result in the Json format. Skip irrelated calculation steps.
        the json contains 'spark_data_lineage`, which is a list containing:
        `original_target_result`: targe result table name or file name of the original spark code,
        `original_source_data`: the list of table names or source file names used to get the target result,
        `revised_target_result`: targe result table name or file name of the revised spark code,
        `revised_source_data`: targe result table name or file name of the revised spark code,
        `transformation_change`:show the difference between the `original_target_result` and `original_target_result`. And explain why they differs by listing the changes in the transformation process. 
                                Show the differences only by listing `change 1`, `change 2` and so on.
    2. Consider the input data from DB or files as source_data. consider the calcaluted data that is shown to user or saved as the target_result.
    3. The intermediate view or table or dataframe cannot be viewed as source_data.
    4. Give the json result only
    """
    try:
        client = OpenAI(
            api_key=qwen_api_key, 
            base_url=qwen_openai_sdk_base_url,
        )
        completion = client.chat.completions.create(
            model=llm_model,
            messages=[
                {'role': 'system', 'content': """You are a Spark SQL expert. 
                 Given two Spark SQL code, one is `Original Spark SQL Code`, the other one is `Revised Spark SQL Code`, 
                 Please help user to identify the changes between these two spark sql codes.
                 """},
                {'role': 'user', 'content': msg}],
            temperature = 1,
            )
        pattern = r'```json(.*?)```'

        matches = re.findall(pattern, completion.choices[0].message.content, re.DOTALL)
        return {'result': matches[0]}
    except:
        return {'result': "```json\n\n```"}

@app.route('/extract_data_lineage', methods=['POST'])
def extract_table():
    data = request.get_json()
    SQL = data.get('SQL')
    llm_model = data.get('llm_model', 'qwen2.5-72b-instruct')
    result = extract_data_lineage_from_spark_sql(SQL, llm_model)
    return jsonify(result)

@app.route('/compare_spark_code', methods=['POST'])
def compare_spark_code():
    data = request.get_json()
    original_sql_code = data.get('original_sql_code')
    revised_sql_code = data.get('revised_sql_code')
    llm_model = data.get('llm_model', 'qwen2.5-72b-instruct')
    result = compare_spark_code_difference(original_sql_code, revised_sql_code, llm_model)
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)