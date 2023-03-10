# DEMAN4 - GROUP 1 - CUP OF JOY - ETL pipeline




## **Team members:**  



- Ramon Almeida
- Derek Tak
- Ghazala Rehman
- Subha Vivekanandan
- May Alavi


## **Project description**

### **The problem the project was trying to solve:**

The client's business is experiencing issues with collating and analysing the data they are producing at each branch, as their technical setup is limited.
The software currently being used only generates reports for single branches which is time consuming to collate data on all branches.
Gathering meaningful data for the company on the whole is difficult, due to the limitations of the software
The company currently has no way of identifying trends, meaning they are potentially losing out on major revenue streams.



### **The requested solution from the client:**

A fully scalable ETL (Extract, Transform, Load) pipeline to handle large volumes of transaction data for the business. This pipeline is to collect all the transaction data generated by each individual café and place it in a single location

![Group1- Cup of Joy](https://user-images.githubusercontent.com/115186875/213158551-7088413a-28fc-4b5a-be90-b828b234d2ca.jpg)



## **How the pipeline will be used by the client:**  
  
    
The client will have 3 csv files uploaded to AWS, at 8pm everyday, for every branch, which is being stored into the Cafe Data S3Bucket. The tranformation  Lambda will be triggered by the S3 event which is the cafe file being uploaded to the bucket. The Lambda will extract the csvs and will transform them. It will then send the transformed data to the Transformed Data S3Bucket. As soon as the transformed data is uploaded to the Transformed Data Bucket, the this will constitue another S3 event which will trigger the load Lambda which will load the data into Redshift. The client will then be able to visualise, query and monitor the date using Grafana and Metabase.






## **The technologies that we used:**


### **AWS**

-_Lambda:_ enabled us to apply custom logic to the pipeline and works in real-time.

-_S3 bucket:_ a durable and easy to navigate tool which enabled us to maintain our pipeline entirely on AWS.

-_Redshift:_ a scalable and cost-effective database which was appropriate for this pipeline as the data was continuously growing.

-_EC2:_ enabled containers consisting of external technologies to be hosted and accessed by all group members



### **Other**

-_Grafana:_ allows for bringing multiple data sources into one location which is why we used it for lambda metrics however it was not as effective in connecting to redshift.

-_Metabase:_ easy to navigate and user-friendly tool to visualise the redshift database by using integrated SQL queries.


## **Some of the challenges that we faced and features we hope to implement in the future:**


### **Challenges**

-Having to rewrite our structure and code entirely at the end of Sprint1.</br>
-Not having enough time for consistent and regular code reviews.</br>
-Group struggled in ensuring all members get sufficient exposure and practice with the key elements of the pipeline equally.</br> 
-Struggled with using GitHub in a consistent manner and as was intended for the project.</br>




### **Future enhancements**

-Implementing a queue system.</br>
-Using grafana for data visualisation successfully.</br>
-Optimising the Lambda code.</br>
-Using a test-driven approach.</br>











