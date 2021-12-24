# ETL Data Pipeline

## **Project Context**    
The main dataset being used contains academic scores for student athletes on NCAA Division 1 teams. The granularity of the data is at the school, sport, and gender level. The goal for this project is to determine if the academic scores for sports teams are correlated with the physicality of the sport. In other words, is there a correlation between contact sports and poor academic performance.

The data was extracted from different sources (csv, web scraping), cleaned and transformed to uniformity, and then loaded into a PostgreSQL database according to the below star schema.

## **Using the code**  
Create and activate virtual environment, then install the dependencies (Powershell)  
**Note:** venv_name is the name of your virtual environment
```
PS C:\> python -m venv venv_name
PS C:\> venv_name\Scripts\Activate.ps1
PS C:\> pip install -r packages.txt
```

To create PostgreSQL database and dimension and fact tables according to the below star schema, run the **create_star_schema.py** file.  
```
PS C:\> python create_star_schema.py
```
![alt_text](https://github.com/oiannace/ETL-pipeline/blob/master/star_schema.png?raw=true)  

Finally, to execute the ETL (Extract, Transform, Load) pipeline and populate the data warehouse according to the above star schema, run the **ETL_pipeline.py** file.
```
PS C:\> python ETL_pipeline.py
```
