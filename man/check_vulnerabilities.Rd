\name{check_vulnerabilities}
\alias{check_vulnerabilities}
\title{Check for vulnerabilities in package dependencies}
\usage{
check_vulnerabilities(create_issue = TRUE, severity_threshold = "critical",
                     output_file = NULL)
}
\arguments{
\item{create_issue}{Logical. If TRUE, creates an issue if critical vulnerabilities are found.
Default is TRUE.}

\item{severity_threshold}{Character. Minimum severity level to report. Options are "critical",
"high", "medium", "low". Default is "critical".}

\item{output_file}{Character. Path to save the vulnerability report. If NULL, no file is saved.
Default is NULL.}
}
\value{
A data frame containing the vulnerability report.
}
\description{
This function checks for vulnerabilities in the package dependencies using the OSS Index
via the oysteR package. It helps ensure ISO compliance by identifying security issues
in the dependencies.
}
\examples{
\dontrun{
# Check for critical vulnerabilities and create an issue if found
check_vulnerabilities()

# Check for high or critical vulnerabilities but don't create an issue
vuln_report <- check_vulnerabilities(create_issue = FALSE, severity_threshold = "high")

# Save the vulnerability report to a file
check_vulnerabilities(output_file = "vulnerability_report.csv")
}
}