# Dependencies

- beautifulsoup4  
- datefinder  
- dateparser  
- pandas  
- requests  
- tqdm  

# Usage

## Filtering new data

1. Go to the AEA website and download the list of job postings in native XLS format. Open it in excel and save it as data/aea.csv.
2. Go to the EJM website and download the list of job postings in csv format. Save it to data/ejm.csv.
3. Open `config/exclude.toml` and adjust to your liking.
4. Run `clean.sh`, which will filter out undesired listings from the AEA and EJM datasets. You may be interested in the --getlinks option, which tries to extract application links from the ad descriptions on the EJM website in case they are not available in ejm.csv (fill `config/ejm_login.toml` with your EJM login if you want to do this).
5. There will now be a file in the root directory called `to_admin.csv`, where the columns are formatted in the department's preferred style and only academic postings are considered. Other useful outputs (especially `output/verbose/*/verbose.csv`, which list ALL of the job postings) can be found in the `output` folder.
