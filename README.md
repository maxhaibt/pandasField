# pandasField
![field](https://github.com/maxhaibt/pandasField/blob/main/readme-images/field-logo.png)
![pandas](https://github.com/maxhaibt/pandasField/blob/main/readme-images/pandas-logo.png)
 Python pandas-based data-science functions for manipulating, extracting and analysing archaeological data recorded with the field research documentation system([Field](https://github.com/dainst/idai-field)).

## Only Developer Usage
Document Data using Field-Desktop
In your python environment (i.e. Anaconda) 
install these standard python-modules:
jupyternotebook
pandas
numpy
difflib 
Download this Repository
Open the pandasField folder in the development-environment of your choice (i.e. VS-Code)
Create a file /config/config.json as a copy of /config/template_config.json
Adapt the values in the config.json for your Field-database.

Based on the jupyternotebook:

OldData2Field_exampleUrukProject.ipynb you can figure out how to use the functions of /lib/pdfield.py to import messy data into your Field-project. Mind the specific mappings defined in /example_mappings_messydata/... to manipulate your values.

FieldAnalysis_exampleUrukProject.ipynb uses the functions of /lib/pdfield.py, in order to visualise and analyse the data in your Field-database. Use the code as a basis for your own analysis.



