# Apache-Kafka
How to run project:
- Set terminal position to folder with docker-compose.yaml
- Run: docker-compose up -d 
- Run both producer and consumer from IntelliJ
- Run curl: curl --location --request POST 'http://localhost:8082/transactions'
- Depending on testing needs, change file name in TransactionController (existing files are located in resources folder)
  
How to stop project:
- Stop both producer and consumer from Intellij
- From terminal run:
  
  docker-compose down

Results:

![Kafka1000](https://github.com/NikolinaTomic/Apache-Kafka/assets/44821513/e86be91d-1427-46ca-99cd-40cfca0bdbad)
![Kafka10k](https://github.com/NikolinaTomic/Apache-Kafka/assets/44821513/9a726b24-5ab5-4a90-931a-0df93a9b82ef)
![Kafka100k](https://github.com/NikolinaTomic/Apache-Kafka/assets/44821513/1b71bfcf-a0e7-45e8-8872-456c772e27c1)
![KafkaMillion](https://github.com/NikolinaTomic/Apache-Kafka/assets/44821513/27b00383-887d-4919-98a9-16d8535d4ceb)


