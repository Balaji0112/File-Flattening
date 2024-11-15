# Get the start time
start_time <- Sys.time()

# Your code here
options(repos = c(CRAN = "https://cloud.r-project.org"))

if (!requireNamespace("jsonlite", quietly = TRUE)) install.packages("jsonlite")
if (!requireNamespace("tidyverse", quietly = TRUE)) install.packages("tidyverse")
if (!requireNamespace("urltools", quietly = TRUE)) install.packages("urltools")
if (!requireNamespace("httr", quietly = TRUE)) install.packages("httr")
if (!requireNamespace("parallel", quietly = TRUE)) install.packages("parallel")
if (!requireNamespace("doParallel", quietly = TRUE)) install.packages("doParallel")

# Load libraries
library(jsonlite)
library(tidyverse)
library(urltools)
library(httr)
library(parallel)
library(doParallel)

# Function to get IP address
get_ip <- function(domain) {
  tryCatch({
    response <- GET(paste0("https://dns.google/resolve?name=", domain), timeout(5)) # nolint
    if (status_code(response) == 200) {
      content <- content(response, "parsed")
      if (length(content$Answer) > 0) {
        return(content$Answer[[1]]$data)
      }
    }
    return(NA)  # Return NA if no IP found or error occurred
  }, error = function(e) {
    return(NA)  # Return NA in case of any error
  })
}

# Read the JSON data from the external file
json_data <- fromJSON("response.json")

# Flatten the data
flattened_data <- json_data$notices %>%
  unnest(works) %>%
  unnest(infringing_urls) %>%
  rename(infringing_url = url) %>%
  mutate(
    topics = map_chr(topics, paste, collapse = "; "),
    copyrighted_url = map_chr(copyrighted_urls, ~ .x$url[1]),
    infringing_domain = domain(infringing_url)
  )

# Set up parallel processing
num_cores <- 64
cl <- makeCluster(num_cores)
registerDoParallel(cl)

# Get unique domains and fetch their IPs in parallel
unique_domains <- unique(flattened_data$infringing_domain)
cat("CSV Started generating... Please wait program is fetching IPs for unique domains\n")

domain_ips <- foreach(domain = unique_domains, .combine = c, .packages = c("httr", "jsonlite")) %dopar% {
  setNames(get_ip(domain), domain)
}

# Stop the cluster
stopCluster(cl)

# Remove NA values from domain_ips
domain_ips <- domain_ips[!is.na(domain_ips)]

# Add IP addresses to the main dataframe
csv_data <- flattened_data %>%
  select(
    id,
    type,
    title,
    date_sent,
    date_received,
    topics,
    sender_name,
    principal_name,
    recipient_name,
    work_description = description,
    infringing_url,
    copyrighted_url,
    infringing_domain
  ) %>%
  mutate(
    infringing_ip = domain_ips[infringing_domain]
  )

# Remove columns where all values are NA
csv_data <- csv_data %>%
  select(where(~ !all(is.na(.))))

# Write to CSV
write.csv(csv_data, "flattened_dmca_data.csv", row.names = FALSE)

# Generate interesting summarizations

# 1. Top 10 domains with the most DMCA notices
top_domains <- csv_data %>%
  group_by(infringing_domain) %>%
  summarise(
    notice_count = n(),
    unique_copyrighted_urls = n_distinct(copyrighted_url)
  ) %>%
  arrange(desc(notice_count)) %>%
  head(10)

write.csv(top_domains, "top_10_infringing_domains.csv", row.names = FALSE)

# 2. Distribution of DMCA notices over time
time_distribution <- csv_data %>%
  mutate(date_sent = as.Date(date_sent)) %>%
  group_by(date_sent) %>%
  summarise(notice_count = n()) %>%
  arrange(date_sent)

write.csv(time_distribution, "dmca_notices_time_distribution.csv", row.names = FALSE)

# 3. Top copyright holders and their most frequently reported infringing domains
top_copyright_holders <- csv_data %>%
  group_by(principal_name) %>%
  summarise(
    notice_count = n(),
    top_infringing_domain = names(which.max(table(infringing_domain))),
    unique_infringing_domains = n_distinct(infringing_domain)
  ) %>%
  arrange(desc(notice_count)) %>%
  head(20)

write.csv(top_copyright_holders, "copyright_holders_rank_wise.csv", row.names = FALSE)

# Get the end time
end_time <- Sys.time()

# Calculate the duration
execution_time <- end_time - start_time

# Print the execution time
cat("Program finished in", execution_time, "seconds.\n")
