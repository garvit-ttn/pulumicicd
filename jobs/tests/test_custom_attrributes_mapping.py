from cvmdatalake import landing
import re

def test_generate_glue_apply_mapping():
    mappings = [
        (c.name.upper(), c.data_type, move_numbers_to_end(c.name), c.data_type) for c in landing.CustomAttributes
    ]
    print(mappings)

def test_generate_staging_ca():
    mappings = ""
    for c in landing.CustomAttributes:
        mappings = mappings + "&&& " +move_numbers_to_end(c.name) + "### " +  c.data_type + "$$$ "

    print(mappings)



def move_numbers_to_end(name: str):
    name = name.lower()
    name = name.replace("ca_","")

    # Extract the numbers at the beginning of the string
    match = re.match(r'^(\d+)(_*)(.*)$', name)
    if match:
        numbers = match.group(1)
        underscore = match.group(2)
        remaining_string = match.group(3)
        return remaining_string + underscore + numbers
    else:
        return name

