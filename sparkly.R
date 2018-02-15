# Connect to Spark
library(sparklyr)
library(dplyr)
library(ggplot2)
Sys.setenv(SPARK_HOME="/usr/lib/spark")
config <- spark_config()
sc <- spark_connect(master = "yarn-client", config = config, version = '1.6.2')


tbl_cache(sc, 'hr_data')
hr_data <- tbl(sc, 'hr_data')

hr_data %>% head()

# Remove NA rows
hr_data <- hr_data %>% na.omit

## Exploratory analysis

# What is the average satisfaction across all
# the departments? Also what is the total number 
# of employees in each department?
df1 <- hr_data %>% 
  group_by(department) %>%
  summarise(avg_satidfaction = mean(satisfaction),
            n_employees = n())

# What is average satisfaction for 
# people who have quit/not quit at 
# each salary level?
df2 <- hr_data %>% 
  group_by(salary,quit) %>%
  summarise(avg_satisfaction = mean(satisfaction))

df1 %>% head(15)
df2 %>% head(15)


ggplot(hr_data %>% collect()) +
  geom_point(aes(x=last_eval,y=satisfaction),color='royalblue',alpha=0.75) +
  ggtitle("Last Evaluation vs Satisfaction") +
  xlab("Last Evaluation") +
  ylab("Satisfaction") +
  theme(plot.title = element_text(hjust = 0.5))

ggplot(hr_data %>% collect()) +
  geom_density(aes(x=satisfaction,fill=as.factor(quit)), alpha=0.7) +
  labs(fill="0=Stayed \n 1=Quit") +
  ggtitle("Density of Satisfaction For Employees") +
  xlab("Satisfaction") +
  ylab("Density") +
  theme(plot.title = element_text(hjust = 0.5))

## Predictive Analytics

# Train - Test Split
data_partition <- hr_data %>%
  sdf_partition(train = 0.7, test = 0.3)

# Fit Model
logistic_regression_fit <- data_partition$train %>%
  ml_logistic_regression(quit ~ satisfaction + last_eval + department + salary)

# Test Model
predictions <- sdf_predict(logistic_regression_fit, data_partition$test) %>%
  collect()

# Get Accuracy
mean(predictions$predicted_label == predictions$label)
# 77.5 % accurate
