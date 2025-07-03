# biggr

This is an open-source R package for the statistical analysis of building's data within the framework of [BIGG project](https://www.bigg-project.eu). It contains a set of AI toolbox that allows the clustering, classification and modelling of building time series and its metadata. This package contains the needed functionalities to take advantage of the [BIGG Ontology](http://www.github.com/biggproject/Ontology), thus the authors recommends its usage when elaborating your own data pipelines. This would generate a bigger chance of re-usage among different companies/users.

## How to install?
Once R is installed (>=4.1 is recommended), execute the following sentences to install all dependent libraries:
```
install.packages(c("devtools","pastecs","purrr","arules","glmnet","ranger","rdflib",
"magrittr","parsedate","lubridate","readr","dplyr","tidyr","tibble","zoo",
"roll","padr","quantreg","testthat","kernlab","fastDummies","caret","penalized",
"GA","parallel","mgcv","qgam","FinCal","onlineforecast","matrixStats","gratia")
# Additionally, is recommended to install libraries that are often used in pipelines based on 'biggr' functions
install.packages(c("plotly","ggplot2","carrier","mlflow","mongolite","gridExtra"))
# For vulnerability scanning (ISO compliance)
install.packages("oysteR")
```
Then, install the package through the GitHub installer if you want to use the last version of the code, or install an stable release located in the [releases repository](https://www.github.com/biggproject/biggr/releases) directly using the install.packages command.

```
# Install development version from GitHub 
devtools::install_github("biggproject/biggr")
# Install stable version from the source package (*.tar.gz) 
install.packages(<.tar.gz file>, repos = NULL, type ="source")
```

## ISO Compliance - Vulnerability Checking

The package includes functionality to check for vulnerabilities in dependencies, which is important for ISO compliance. To use this feature:

```r
# Basic usage - checks for critical vulnerabilities and creates an issue if found
biggr::check_vulnerabilities()

# Check for high or critical vulnerabilities but don't create an issue
vuln_report <- biggr::check_vulnerabilities(create_issue = FALSE, severity_threshold = "high")

# Save the vulnerability report to a file
biggr::check_vulnerabilities(output_file = "vulnerability_report.csv")
```

### Automated Vulnerability Scanning

The package repository includes a GitHub Actions workflow that automatically scans for vulnerabilities on a weekly basis. This workflow:

- Runs every Monday at 00:00 UTC
- Scans for high and critical vulnerabilities in all dependencies
- Creates a GitHub issue if vulnerabilities are found
- Uploads a detailed vulnerability report as a workflow artifact

This automated scanning helps ensure continuous monitoring of security issues without manual intervention.

The vulnerability checking functionality helps ensure that your project meets security requirements by identifying and reporting vulnerabilities in package dependencies.

## License
This R package is licensed under the EUPL License. It also depends on other popular open-source R libraries, from which it will retain their licenses.

## Authors

- Gerard Mor - gmor@cimne.upc.edu
- Aleix Badia 
- Eloi Gabaldón - egabaldon@cimne.upc.edu
- Jordi Carbonell - jordi@cimne.upc.edu
- Stoyan Danov - sdanov@cimne.upc.edu
- Florencia Lazzari 
- Gerard Laguna - glaguna@cimne.upc.edu
- Marc Girona - mgirona@cimne.upc.edu
- Jordi Cipriano - cipriano@cimne.upc.edu
- Riccardo De Vivo
- Manu Lahariya

Copyright (c) 2022 Gerard Mor, Aleix Badia, Eloi Gabaldón, Jordi Carbonell, Stoyan Danov, Florencia Lazzari, Gerard Laguna, Marc Girona, Jordi Cipriano, Riccardo De Vivo, Manu Lahariya
