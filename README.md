#
![field](https://github.com/maxhaibt/pandasField/blob/main/readme-images/pandasField-logo.png)

 Python pandas-based data-science functions for manipulating, extracting and analysing archaeological data recorded with the field research documentation system([Field](https://github.com/dainst/idai-field)).

## Developer Usage
1. Document Data using Field-Desktop
2. In your python environment (i.e. Anaconda) install these standard python-modules:
    * jupyternotebook
    * pandas
    * numpy
    * difflib 
3. Download this Repository
4. Open the pandasField folder in the development-environment of your choice (i.e. VS-Code)
5. Create a copy of file /config/template_config.json and name it /config/config.json.
6. Adapt the values in the config.json for your Field-database.
7. Based on the jupyternotebooks

## Code-Base
In OldData2Field_exampleUrukProject.ipynb you can figure out how to use the functions of /lib/pdfield.py to import messy data into your Field-project. Mind the specific mappings defined in /example_mappings_messydata/... to manipulate your values.

The FieldAnalysis_exampleUrukProject.ipynb uses the functions of /lib/pdfield.py, in order to visualise and analyse the data in your Field-database. Use the code as a basis for your own analysis. To produce plots like these and more:

![field](https://github.com/maxhaibt/pandasField/blob/main/readme-images/plot_wes_findsinperiods.png)




