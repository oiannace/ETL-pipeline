# ETL Data Pipeline

## **Getting started**  
Create and activate virtual environment, then install the dependencies (Powershell)  
**Note:** venv_name is the name of your virtual environment
```
PS C:\> python3 -m venv venv_name
PS C:\> venv_name\Scripts\Activate.ps1
PS C:\> pip3 install -r packages.txt
```

To create PostgreSQL database and dimension and fact tables according to the above star schema, run the create **create_star_schema.py** file.


## **Project Context**    
The main dataset being used contains academic scores for student athletes on NCAA Division 1 teams. The granularity of the data is at the school, sport, and gender level. The goal for this project is to determine if the academic scores for sports teams are correlated with the physicality of the sport. In other words, is there a correlation between contact sports and poor academic performance.

## **ETL (Extract, Transform, Load)**  
The data was extracted from 2 csv files, as well as scraped from a wiki table.

The data from different sources 
