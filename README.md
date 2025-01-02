<h1>Analyzing users with similar preferences to make personalized recommendations using Collaborative Filtering Using Cosine Similarity in MapReduce.</h1>
Objective: Collaborative filtering identifies users with similar preferences to make personalized recommendations. The cosine similarity algorithm helps measure the similarity between users based on their movie ratings, forming the basis for user-based recommendation systems.
</br>
<h2>Use Case:</h2>
<p>Fraud Detection in Financial Services
  <br/>

Problem: 
  <br/>
A financial service provider wants to identify users with abnormal spending patterns.
<br/>

Solution: Use cosine similarity to find users with spending patterns that deviate significantly from the norm.
<br/>

•	Flag users with low similarity scores for further investigation.

<br/>

Mapper (UserMovieMapper):<br/>
 Reads input data and emits userId,movieId as the key and rating as the value.
Reducer (CosineSimilarityReducer):<br/>
 Groups all ratings for each user-movie pair.
<br/>

•	Computes the cosine similarity between users by calculating the dot product and magnitudes of their rating vectors.
<br/>

Driver: Sets up the job and specifies the mapper, reducer, and I/O formats.<br/>

The Cosine Similarity Reducer is a part of the MapReduce workflow that calculates the similarity between users based on their movie preferences using the cosine similarity algorithm. This algorithm measures how similar two vectors (in this case, user ratings) are, regardless of their magnitude.
<br/>
 Input to Reducer:<br/>

•	The reducer receives all ratings for a specific movieId and groups them by userId.
<br/>
•	For example:
Input Key: movieId_1 <br/>

Input Values: [user1:4.0, user2:3.5, user3:5.0] <br/>

<br/>
 Prepare Ratings Data:
 <br/>

•	The reducer organizes the ratings into a map where each user is associated with their rating for the current movie.
<br/>

Calculate Pairwise Similarity:<br/>

•	The reducer computes pairwise cosine similarity for all combinations of users who have rated the same movie.<br/>

•	For instance, it calculates the similarity between user1 and user2, user1 and user3, and so on.
<br/>

Dot Product and Magnitudes:<br/>

•	For each pair of users:<br/>

o	Compute the dot product of their rating vectors.<br/>

o	Calculate the magnitude of each user’s vector.<br/>

o	Compute the cosine similarity using the formula.<br/>


</p>
<br/>
<pre>
  scp /mnt/c/Users/tarun/Documents/GitHub/INFO_7250_Big_data/final_project/CollaborativeFiltering/target/CollaborativeFiltering-1.0-SNAPSHOT.jar hdoop@tarunsankhla:~/CollaborativeFiltering-1.0-SNAPSHOT.jar 

  hadoop jar ~/CollaborativeFiltering-1.0-SNAPSHOT.jar edu.neu.csye6220.collaborativefiltering.CollaborativeFiltering /final_project/ratings.csv /user/hdoop/collaborative_filtering_output
  
  hdfs dfs -cat /user/hdoop/collaborative_filtering_output/part-r-00000 | head
  
  hdfs dfs -ls /user/hdoop/collaborative_filtering_output
  
  hdfs dfs -rm -r /user/hdoop/collaborative_filtering_output

</pre>
 
![image](https://github.com/user-attachments/assets/e4c5a816-c5a2-4114-96e9-56c691baf14e)
<br/>
