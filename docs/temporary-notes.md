java --version
mvn --version

https://maven.apache.org/ref/3.0.4/maven-model-builder/super-pom.html

https://maven.apache.org/wrapper/
https://www.mojohaus.org/exec-maven-plugin/


C:\Program Files\Java\apache-maven-3.9.3


Πάω σε κάθε project που έχει avro και εκτελώ: mvn clean compile


mvn spotless:check
mvn spotless:apply


Για dropwizard:
mvn clean package
java -jar path-to-jar.jar server configuration.yml


python -m iotvm.main basic_generator
flask --app iotvm.server run

.\thrift-0.18.1.exe -r --gen py --out ..\..\iotvm-eventengine-extensions .\fabrication_forecasting.thrift
.\thrift-0.18.1.exe -r --gen java --out ..\..\iotvm-eventengine\src\main\java .\fabrication_forecasting.thrift

Για test:
```mvn test```

Task (routine):
- Flush Kafka topics.
- Delete DB records.
Ώστε να το ξανατρέχω σαν να ξεκινά από την αρχή.

http://localhost:9000

| Name            | Internal Ports | External Ports |
|-----------------|----------------|----------------|
| zookeeper       | 2181           | 10100          |
| kafka           | 9092, 29029    | 10101          |
| schema registry | 8081           | 10102          |
| mongodb         | 27017          | 10103          |
| kafdrop         | 9000           | 9000           |
| gateway         | 9001           | 9001           |
| event engine    | 9002           | 9002           |
| extensions      | 9003           | 9003           |

---

# Demonstration Instructions

```
npm run dev
docker-compose -f iotvm-docker/docker-compose.yml up -d
python -m iotvm_extensions.main start_server
java -jar target/gateway.jar server configuration.yml
java -jar target/eventengine.jar
python -m iotvm.main start_generator
# Του δίνουμε λίγο χρόνο ώστε να παραχθούν δεδομένα. Για τους πρώτους κύκλους το forecasting δεν είναι διαθέσιμο.
python -m iotvm_extensions.main ensure_forecasters
```

# Practical Implications

- Kafka Stream will not start if log4j is not configured.
- Πώς αντιμετωπίζουμε το ζήτημα της εξέλιξης των schemas των events.
  Π.χ. η προσθήκη ενός πεδίου, η αφαίρεση ενός πεδίου. Τι γίνεται με τα υπάρχοντα; Τι γίνεται με τα καινούργια; Τι γίνεται όταν η αλλαγή γίνεται σε runtime;
- Όταν υπάρχει σφάλμα στο Kafka Streams, σταματέι το πρόγραμμα. Εμείς το κάνουμε configure να συνεχίζει με σφάλματα αλλά να μας τα κάνει report.
- Το schema registry και τα specific records εξυπηρετούν συγκεκριμένα πράγματα. Εμείς δε θέλουμε κεντρικοποίηση. Θέλουμε avro αλλό όχι registry. Άρα πρέπει να φτιάξουμε ένα custom lib πάνω στο υπάρχον που να μη χρειάζεται παρέμβαση από το schema registry.
- Το schema registry, δε θα με χαλούσε αν υποστήριζε references. (νομίζω τα υποστηρίζει.... άλλο είναι το θέμα....)
- kafka streams suppress surprise https://rgannu.medium.com/kafka-streams-suppress-surprise-975d4f56870c
- `suppress` without `tostream` gives wrong results (δες το επόμενο!)
- ΠΡΟΣΟΧΗ: τα duplicates μετά το `suppress` μπορεί να οφείλονταν στο ίδιο size & advanceBy που ήταν ένα λεπτό. Πρέπει να δω αν πρέπει το advanceBy να είναι υποχρεωτικά μεγαλύτερο... Ή το αντίστροφο.
- ΣΧΕΤΙΚΑ με το suppress surprise: Χρειάζομαι control events (https://jeqo.github.io/posts/2022-06-17-kafka-streams-tick-event-time-with-control-messages/). Για να γίνει αυτό πρέπει να αλλάξω τα IBOs ώστε να είναι πολυμορφικά αλλά με κάποιο hint-field.
- Η αλήθεια είναι στη βάση του Kafka Streams μπορούν να χτιστούν πολύπλοκες τοπολογίες που υπό άλλες συνθήκες δε θα ήταν εφικτές. Ίσως δε βλέπουμε έντονα την αξία του Kafka Streams λόγω του απλού παραδείγματος. Σίγουρα όμως, ως βασική βιβλιοθήκη, χρειάζεται επέκταση, είτε με ksql είτε με κάποια υλοποίηση για γνωστά και ευρέως διαδομένα CEP patterns.
- Ίσως η δουλειά που κάνει το Gateway είναι το πραγματικό ingestion και ίσως πρέπει να το μεταφέρω στο event engine και να αλλάξω ελαφρώς τη ροή. Θα χρειαστούμε και νέο topic.
- Η κλάσεις που υλοποιούν το interface `Aggregator` δεν μπορούν να έχουν πληροφορίες για το χρονικό παράθυρο. Ούτε το κλειδί (`key`) περιλαμβάνει αντίστοιχες πληροφορίες. Είναι το key που έχω επιλέξει. Αυτό σημαίνει δύο πράγματα:
  - 1. Αν είναι ανάγκη, με βάση τα parameters και τα timestamps να υπολογίσω σε ποιο παράθυρο βρίσκομαι.  
  - 2. Να κάνω το aggregation, και στη συνέχεια να χρησιμοποιήσω άλλη μέθοδο για να κάνω επιπλέον ενέργειες.   
  - Σίγουρα αυτό εκ πρώτης όψεως φαίνεται περιοριστικό αλλά δεν είναι. Η βιβλιοθήκη Kafka Streams είναι γενικευμένη και περιλαμβάνει βασικά building blocks για να καλύπτουν όλο το εύρος του stream processing.
- ΠΡΟΣΟΧΗ: το suppress σε πραγματικά περιβάλλοντα μπορεί να μαζέψει χιλιάδες μηνύματα μέχρι να διάλεξει το τελευταίο. Με αποτέλεσμα να σκάσει το πρόγραμμα. Εκεί υπάρχουν στρατηγικές διαχείρισης. π.χ. emit early αν φτάσει κάποιο όριο.
- Χρειάζεται προτυποποίηση στο πώς αποθηκεύω και διαχειρίζομαι πληροφορίες για sensors. Schema, πεδία, specs, κλειδιά, etc.
- Χρειάζεται προτυποποίηση για distributed identifiers and names αναφορικά με class names, keys, schemas, event types, topics. Δεν υποστηρίζουν όλες οι τεχνολογίες τα ίδια conventions σχετικά με την ονοματολογία. Π.χ. τα durations `PT20.345S` και `-PT-6H+3M` δε μπορούν να χρησμοποιηθούν εύκολα ως ονόματα σε ένα MongoDB collection ή σε topic.
- Τα composite transformation μπορούν να τρέξουν σε ένα σύστημα ή κατανεμημένα. Επίσης, μπορούν να δημιουργηθούν πολλά kafka streams applications που είτε μπορούν να τρέχουν σε ένα σύστημα είτε κατανεμημένα. Το μοντέλο μας είναι εξαιρετικά ευέλικτο!  

- https://jira.mongodb.org/browse/JAVA-3372?attachmentOrder=desc

-  2023-08-29T14:31:00Z vs 2023-08-29T14:31:10Z.  Window is agnostic if start/end boundaries are inclusive or exclusive; this is defined by concrete window implementations. Convention: start - end minus 1 millisecond (EVERYWHERE!)

# Tasks:

- Utility that reset and flushes
- Τα events πρέπει να έχουν clientId (π.χ. UUID). Πολύ σημαντικό για tracing, correlation, observability και monitoring γενικότερα. 
- Τα events πρέπει να έχουν timestamps map με τις χρονικές στιγμές των σταδίων των κύκλων επεξεργασίας
- Event Engine: load configuration from file?
- Splitting DLQ for non-supported physical quantities
- Ίσως χρειάζεται μια βασική επαλήθευση η επιλογή του timestamp. π.χ. εμπιστεύεσαι 100% την πηγή; Ναι αν είναι authenticated/authorized. Αλλιώς, μόνο για τις ανάγκες του τεστ και της προσομοίωσης.
- Create a utility that returns a toString representation of auto-generated avro IBOs.
- Τι γίνεται αν το `sensorId` δεν είναι έγκυρο ή δεν αντιστοιχεί σε υπαρκτό sensor;
- Περιγραφή της κοινής δομής των μετασχηματισμών
- stream time / wall time / etc -> internal clock controller - για να κάνω πάντα το stream time να προχωράει. (προαιρετικό)
- Add to docs: Τα events μπορεί να έχουν στιγμιότυπα ή προβολές στιγμιοτύπων που αντανακλούν τη δεδομένη χρονική στιγμή. Στο μέλλον μπορεί να αλλάξουν... Επίσης, μπορεί να έχουμε μόνο ένα υποσύνολο των χαρακτηριστικών των στιγμιοτύπων πράγμα που είναι φυσιολογικό σε event-driven distributed architectures.

# Future

- `System.currentTimeMillis()` vs `Instant.now().toEpochMilli()`
- Χρήση analytics program στο οποίο θα αποθηκεύω timestamped counters όταν υπάρχει input και output ώστε να βλέπω διάφορες μετρικές όπως ρυθμό εξυπηρέτησης, επιτυχή εκτέλεση composite transformation, κ.τ.λ.
- Evolution chain, ID chain, evolution ID chain, composite transformation lifecycle, etc.

Useful Packages:

```xml
<dependencies>
  <dependency>
    <groupId>de.slub-dresden</groupId>
    <artifactId>urnlib</artifactId>
    <version>[2.0,2.1)</version>
  </dependency>
  <dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>connect-api</artifactId>
    <version>3.5.0</version>
  </dependency>
  <dependency>
    <groupId>org.mongodb.kafka</groupId>
    <artifactId>mongo-kafka-connect</artifactId>
    <version>1.10.1</version>
  </dependency>
</dependencies>
```

Already included packages:  

```xml
<dependencies>
  <dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.15.2</version>
  </dependency>
  <dependency>
    <groupId>com.google.code.findbugs</groupId>
    <artifactId>jsr305</artifactId>
    <version>3.0.2</version>
  </dependency>
</dependencies>
```

# Datasets

https://data.mendeley.com/datasets/wtpt7mssrn/2
https://data.mendeley.com/datasets/3dw54yhhcc/2
https://zenodo.org/record/5793685
https://www.agroforestry.co.uk/forest-garden-greenhouse-project/fgg-climate-data/

# Examples (high quality examples)

- [Quarkus - Using Apache Kafka Streams](https://quarkus.io/guides/kafka-streams)
- 