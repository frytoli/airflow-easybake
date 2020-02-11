#!/usr/bin/env python

from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
import datetime
import logging
import time
import json
import os


'''
The Cake Recipe:
'''
recipe = {
    'required_ingredients' : {
        'eggs'  : 2,
        'flour' : 2,
        'sugar' : 1,
        'butter': 1,
        'water' : 1
    },
    'required_cookwear' : {
        'pan'  : 1,
        'mixer': 1,
        'spoon': 1
    },
    'required_temp' : 350
}
oven_temp = 65


'''
Pantry and Cabinet Access Functions
(Consider these as database communication functions)
'''
# Read ingredients from the pantry
def read_pantry():
    pantry_file = os.path.join(os.environ.get('AIRFLOW_HOME'),'dags','pantry.json')
    with open(pantry_file,'r') as injson:
        pantry = json.load(injson)
    return(pantry)

# Write ingredients to the pantry
def write_pantry(items):
    pantry_file = os.path.join(os.environ.get('AIRFLOW_HOME'),'dags','pantry.json')
    with open(pantry_file,'w') as outjson:
        json.dump(items, outjson, indent=4)

# Read cookwear items from the cabinets
def read_cabinets():
    cabinets_file = os.path.join(os.environ.get('AIRFLOW_HOME'),'dags','cabinets.json')
    with open(cabinets_file,'r') as injson:
        cabinets = json.load(injson)
    return(cabinets)

# Write cookwear items to the cabinets
def write_cabinets(items):
    cabinets_file = os.path.join(os.environ.get('AIRFLOW_HOME'),'dags','cabinets.json')
    with open(cabinets_file,'w') as outjson:
        json.dump(items, outjson, indent=4)


'''
Cake Baking Methods:
'''
# Acquire the necessary ingredients until there are not enough left
def get_ingredients(required):
    # Read items from the pantry
    pantry = read_pantry()
    # Iterate over required ingredients...
    for ingredient in required:
        # If the ingredient is not in the pantry, return False
        if ingredient not in pantry.keys():
            logging.warning('Ingredient {} not found in the pantry'.format(ingredient))
            return(False)
        # Else if there is not enough of the required ingredient in the pantry, return False
        elif required[ingredient] > pantry[ingredient]:
            logging.warning('Not enough of Ingredient {} exists in the pantry to make the recipe'.format(ingredient))
            return(False)
        # Else, take the required amount of the ingredient from the pantry and continue
        else:
            pantry[ingredient] -= required[ingredient]
    # Write updated items to the pantry
    write_pantry(pantry)
    # Return true for success
    return(True)

# Acquire the necessary cookwear items until not enough are clean
def get_cookwear(required):
    # Read items from the cabinets
    cabinets = read_cabinets()
    # Iterate over required cookwear...
    for cookwear in required:
        # If the cookwear item is not in the cabinets, return False
        if cookwear not in cabinets.keys():
            logging.warning('Cookwear item {} not found in the cabinets'.format(cookwear))
            return(False)
        # Else if there is not enough clean cookwear items in the cabinets, return False
        elif required[cookwear] > cabinets[cookwear]:
            logging.warning('Not enough of cookwear item {} has been cleaned to make the recipe'.format(cookwear))
            return(False)
        # Else, take the required amount of the cookwear item from the cabinets and continue
        else:
            cabinets[cookwear] -= required[cookwear]
    # Write updated items to the cabinets
    write_cabinets(cabinets)
    # Return true for success
    return(True)

# Return the unused ingredients to the pantry
def return_ingredients(required):
    # Read items from the pantry
    pantry = read_pantry()
    # Iterate over required ingredients...
    for ingredient in required:
        # All ingredients are taken from the pantry, therefore a key must already exist for it within the pantry dict
        # Return the amount of each ingredient that was taken for the recipe back to the pantry
        pantry[ingredient] += required[ingredient]
        logging.info('Returned {}x ingredient {} to pantry'.format(required[ingredient], ingredient))
    print(pantry)
    # Write updated items to the pantry
    write_pantry(pantry)

# Return the unused cookwear to the cabinets
def return_cookwear(required):
    # Read items from the cabinets
    cabinets = read_cabinets()
    # Iterate over required cookwear...
    for cookwear in required:
        # All cookwear items are taken from the cabinets, therefore a key must already exist for it within the cabinets dict
        # Return the amount of each cookwear item that was taken for the recipe back to the cabinets
        cabinets[cookwear] += required[cookwear]
        logging.info('Returned {}x cookwear item {} to cabinets'.format(required[cookwear], cookwear))
    # Write updated items to the cabinets
    write_cabinets(cabinets)

# Preheat the oven if it hasn't been preheated before
def preheat_oven(required):
    global oven_temp
    if oven_temp < required:
        logging.info('Preheating the oven to 350 degrees')
        time.sleep(45)
        oven_temp = 350
    else:
        logging.info('The oven has already been preheated')

# Bake the cake!
def bake_cake(required):
    global oven_temp
    # If the oven is the correct required temperature, bake the cake
    if oven_temp == required:
        logging.info('Baking the cake now...')
        time.sleep(60)
        logging.info('Ding!!')

# Replenish the pantry with the amount of ingredients required for the recipe
def go_shopping(required):
    # Read items from the pantry
    pantry = read_pantry()
    # Iterate over required ingredients...
    for ingredient in required:
        # If the ingredient is not in the pantry, buy enough for the recipe
        if ingredient not in pantry.keys():
            pantry[ingredient] = required[ingredient]
            logging.info('Purchased {}x new ingredient {}'.format(required[ingredient], ingredient))
        # Else if there is not enough of the required ingredient in the pantry, buy the amount that the recipe calls for
        elif required[ingredient] > pantry[ingredient]:
            pantry[ingredient] += required[ingredient]
            logging.info('Purchased {}x ingredient {}'.format(required[ingredient], ingredient))
    # Write updated items to the pantry
    write_pantry(pantry)

# Wash all the cookwear that was just used
def wash_dishes(required):
    # Read items from the cabinets
    cabinets = read_cabinets()
    # Iterate over required cookwear...
    for item in required:
        logging.info('Wash the {}... Wash the {}...'.format(item, item))
        time.sleep(10)
        cabinets[item] += 1
    # Write updated items to the cabinets
    write_cabinets(cabinets)

# Branch function
def branch(**kwargs):
    # Retrieve the returned values of the get_ingredients and get_cookwear operators
    have_ingredients = kwargs['ti'].xcom_pull(task_ids='get_ingredients')
    have_cookwear = kwargs['ti'].xcom_pull(task_ids='get_cookwear')
    # If both the required ingredient and cookwear needs have been met, continue to mixing the batter
    if have_ingredients and have_cookwear:
        return(['preheat_oven', 'mix_ingredients'])
    # Else if the required ingredient needs have NOT been met AND the cookwear needs have NOT been met, go shopping and wash the dishes
    elif not have_ingredients and not have_cookwear:
        return(['go_shopping', 'wash_dishes'])
    # Else if the required ingredient needs have NOT been met BUT the cookwear needs have been met, go shopping and return the cookwear to the cabinets
    elif not have_ingredients and have_cookwear:
        return(['go_shopping', 'return_cookwear'])
    # Else if the required ingredient needs have been met BUT the cookwear needs have NOT been met, wash the dishes and return the ingredients to the pantry
    elif not have_cookwear and have_ingredients:
        return(['wash_dishes', 'return_ingredients'])


# =========================================================


'''
The DAG
'''
# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=5),
    'task_concurrency': 1
}

# Define the DAG and its operators
with DAG(
        dag_id='easybake',
        description='Baking cakes until we run out of ingredients with a DAG',
        default_args=default_args,
        catchup=False,
        schedule_interval='@daily'
    ) as dag:
    opr_get_ingredients = PythonOperator(
        task_id='get_ingredients',
        python_callable=get_ingredients,
        op_kwargs={'required':recipe['required_ingredients']}
    )
    opr_get_cookwear = PythonOperator(
        task_id='get_cookwear',
        python_callable=get_cookwear,
        op_kwargs={'required':recipe['required_cookwear']}
    )
    opr_return_ingredients = PythonOperator(
        task_id='return_ingredients',
        python_callable=return_ingredients,
        op_kwargs={'required':recipe['required_ingredients']}
    )
    opr_return_cookwear = PythonOperator(
        task_id='return_cookwear',
        python_callable=return_cookwear,
        op_kwargs={'required':recipe['required_cookwear']}
    )
    opr_preheat_oven = PythonOperator(
        task_id='preheat_oven',
        python_callable=preheat_oven,
        op_kwargs={'required':recipe['required_temp']}
    )
    opr_branch = BranchPythonOperator(
        task_id='branch',
        python_callable=branch,
        provide_context=True
    )
    opr_mix_ingredients = BashOperator(
        task_id='mix_ingredients',
        bash_command='echo "Mix mix mix... Mix mix mix..." && sleep 5'
    )
    opr_bake_cake = PythonOperator(
        task_id='bake_cake',
        python_callable=bake_cake,
        op_kwargs={'required':recipe['required_temp']}
    )
    opr_cool_cake = BashOperator(
        task_id='cool_cake',
        bash_command='echo "Cool cool cool... Cool cool cool..." && sleep 5'
    )
    opr_go_shopping = PythonOperator(
        task_id='go_shopping',
        python_callable=go_shopping,
        op_kwargs={'required':recipe['required_ingredients']}
    )
    opr_wash_dishes = PythonOperator(
        task_id='wash_dishes',
        python_callable=wash_dishes,
        op_kwargs={'required':recipe['required_cookwear']}
    )

# Define the DAG structure
[opr_get_ingredients, opr_get_cookwear] >> opr_branch
opr_branch >> [opr_preheat_oven, opr_mix_ingredients] >> opr_bake_cake >> opr_cool_cake >> opr_wash_dishes
opr_branch >> [opr_go_shopping, opr_wash_dishes]
opr_branch >> [opr_go_shopping, opr_return_cookwear]
opr_branch >> [opr_wash_dishes, opr_return_ingredients]
