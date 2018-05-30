# Scout-Crawler
The crawler is a system based on StormCrawler to crawl continuously a specified list of domains completely and hand the crawled web pages to other components via RabbitMQ and notifies when a domain has finished crawling
## Getting Started
These instructions will get you running the system on your local machine for development and testing purposes. 
### Prerequisites
What things you need to install the software and how to install them
#### Required Software:
- Java 1.8
- Storm 1.2.1
- Elasticsearch 6
- Kibana or Grafana (optional to see metrics)
- RabbitMQ (when you want to store the crawled pages somewhere)
#### Required Files
For the following getting started guide it is assumed that you have the following files created in the project path
##### seeds.txt
Create a list of URLs you want to inject into Elasticsearch to be crawled
```
https://books.toscrape.com
https://qoutes.toscrape.com
```
We also track an identifier of the domain through the architecture which can be here specified as an attribute of metadata. The attribute which is tracked has to be separated by the URLs with a tab
```
https://books.toscrape.com   shopId=1234
https://qoutes.toscrape.com   shopId= 54321
```
##### dev.properties
For an easy development and deployment it is handy to use a properties file which is used to substitute parameters in the configuration. Here are some suggested parameters which can be replaced and kept separate for production and development
```yaml
es.address: http://localhost:9200/
fetcher.threads: 10
crawler.memory: 2048
```
##### access-data.yaml
This file is currently used to store all access data and is also referenced in the es-crawler.flux file under resources. Here we store the access data for RabbitMQ
```yaml
#The configuration for out RabbitMQ
config:
  rabbitmq.host: "some.host.de"
  rabbitmq.port: 5672
  rabbitmq.username: "admininstrator"
  rabbitmq.password: "abc"
# necessary as otherwise it will get a null pointer exception
  rabbitmq.ha.hosts: ""
```
#### Storm installation
A tutorial on how to install storm can be found [here](https://www.quora.com/How-do-I-install-a-multi-node-Apache-Storm-on-Ubuntu-16-04)
For comfortable use set the environment variable for Storm
```bash
export PATH=$PATH:/usr/local/storm/bin
```
For comfortable use create startup scripts for the services in /etc/systemd/system/thing.service
##### storm-nimbus.service
```ini
[Unit]
Description=Runs the storm nimbus service
 
[Service]
Type=simple
User=storm
Group=storm
ExecStart=/usr/local/storm/bin/storm nimbus
Restart=on-failure
SyslogIdentifier=storm-nimbus
 
[Install]
WantedBy=multi-user.target
```
##### storm-supervisor.service
```ini
[Unit]
Description=Runs the storm supervisor
 
[Service]
Type=simple
User=storm
Group=storm
ExecStart=/usr/local/storm/bin/storm supervisor
Restart=on-failure
SyslogIdentifier=storm-supervisor
 
[Install]
WantedBy=multi-user.target
```
##### storm-ui.service
```ini
[Unit]
Description=Runs the storm ui service
 
[Service]
Type=simple
User=storm
Group=storm
ExecStart=/usr/local/storm/bin/storm ui
Restart=on-failure
SyslogIdentifier=storm-ui
 
[Install]
WantedBy=multi-user.target
```
### Running the topology
A good introduction to StormCrawler in general can be found [here](http://stormcrawler.net/getting-started/)
To get the crawler running
- Clone the repository
- Change to the directory and build the jar with 
```bash
mvn clean package
```
#### Injecting the URLs into Elasticsearch
```bash
storm jar target/spikeStormCrawler-1.0-SNAPSHOT.jar  org.apache.storm.flux.Flux --local es-injector.flux --filter prod.properties
```
The seed URLs will be read from the seeds.txt in the folder and injected into the status index of Elasticsearch
NOTE: Storm has to be running this includes
- Supervisor
- Nimbus
- UI
#### Running the crawler
```bash
storm jar target/spikeStormCrawler-1.0-SNAPSHOT.jar  org.apache.storm.flux.Flux --remote es-crawler.flux --filter prod.properties
```
This will deploy the crawler on the storm topology where it can be monitored with the Storm UI
NOTE:
In the current topology it is required that RabbitMQ is running with the specified exchange name and routing key. There the crawled pages will be stored. Instead of storing them in RabbitMQ you can use the BPFileBolt to store them as files.
## How it works
The current topology is set up to crawl a specified list of Domains one time. It used Elasticsearch as a storage for the URLs which have either been `DISCOVERED` or `FETCHED`. The crawled pages will be stored in a RabbitMQ. The crawler also notices within 30 seconds when a domain is crawled and sends a message with the shopId of the finished domain to another queue.
### Configuration
The crawler can be configured in the following files
#### es-crawler.flux
This is the file where you configure the topology of the crawler. It states where the configuration files can be found, which Spouts and Bolts will be used and how they communicate via streams.
Instead of the RabbitMQ Bolt to write the web pages to a queue, you could use the BPFileBolt instead which would write them to a file
#### es-injector.flux
This topology is used to inject the specified URLs into Elasticsearch which will then be used for crawling. Injection during the crawling process should not pose a problem as Elasticsearch sorts all URLs by nextFetchDate and groups them into queues according to their host.
#### crawler-conf.yaml
This configuration file is the main configuration for the crawler. Here you can set all parameters which are important like the crawling delay per page, the memory which should be given to the crawler or also which metadata like the shopId should be carried through the topology. See on configuration [this wiki page] (https://github.com/DigitalPebble/storm-crawler/wiki/Configuration) of StormCrawler 
#### es-conf.yaml
This configuration file is the configuration for Elasticsearch. These are the parameters which index should be for example used for status or metrics. 
Scale StormCrawler
 A good inspiration how StormCrawler could be scaled for massive crawls can be found in [this](https://github.com/DigitalPebble/stormcrawlerfight) Benchmark of StormCrawler
### Diagnosis
How StormCrawler is actually working can be found in two places
#### UIs
- Storm UI - When you installed storm and have the standard configuration than you would reach the storm ui on port 8080 where statistics about storm can be found
- Kibana Dashboard - When you follow the [tutorial](https://www.youtube.com/watch?v=KTerugU12TY) you will have configured Elasticsearch and Kibana and will find under Dashboards the dashboard metrics where - -you will find current stats about the fetching process
- Grafana - This is a alternative to Kibana which enables to track even more metrics. See here the [Dashboard](http://digitalpebble.blogspot.com/2018/03/grafana-stormcrawler-metrics-v4.html)
#### Logs
Storm will create logs which you will see in the directory where storm is installed in the directory logs. The most important logs will be 
- **nimbus.log**
This will tell you about when the crawler will be started, stopped or halted
- **worker.log** 
In the folder worker artifacts there will be a worker.log in the directory corresponding to the current typology. Here you can see which pages will be fetched and parsed as well as how the crawler is interacting with Elasticsearch or throws exceptions.
 ## Future feature improvements
#### Injecting automatically URLs:
To not use the method of injecting URLs manually into the topology a improvement would be to program a spout which emits after a specified time URLs into Elasticsearch 
#### Control the Crawler
There could be the feature to control the crawler via another component. This could either be done by a Spring REST Service)from outside by controlling directly Elasticseach or by a component inside storm like a bolt (see the rest-express framework). This component could implement features like 
Aborting the crawling for specified domains by deleting all URLs of the domain in Elasticsearch
Starting the injection of specified domains
also see [this](https://stackoverflow.com/questions/49999111/control-stormcrawler-via-rest) for advantages and disadvantages
#### Use selenium/headless chrome for problematic web shops which need to execute Javascript on the webpage
Integrated in StormCrawler is already the option to use a remoteDriver which controls for example a Google Chrome browser. For this use the tool chromedriver and see the [old Branch](https://github.com/HPI-BP2017N2/Crawler/tree/%236/Use_Selenium_Headless_Chrome_for_problematic_Webshops)
#### Make crawling faster by skipping unnecessary pages
When you are crawling a web shop and are only interested in the articles it might be wise to skip non-article pages like the FAQs or a Blog of the web shop. In order to do this you could find a URL pattern which guarantees the matching of unnecessary pages. A URL filter pattern can be submitted in the src/main/ressources/urlfiltersCustom.json to skip unnecessary pages.
#### Robots.txt parser could be extended to comply to exotic rules 
For example in [viking.de/robots.txt](https://www.viking.de/robots.txt) you can see the rule which is not yet supported
```
# only visit between 23:00 and 06:45 UTC
Visit-time: 2300-0645
```
#### No notification of old finished domains after crawler restart or crash
Currently the components BPFinishedShopBolt has a InMemory HashSet of Domains for which a notification already has been send. When the crawler will be restarted during the process it will send out notifications for all domains which have been finished until that point.
#### Make Elasticsearch Query for FinishedShops more performant
Currently we do an aggregation query which asks for all domains with the number of URL which have the status `DISCOVERED`.  This is quite costly when Elasticsearch contains a lot of URLs. With the help of a custom bolt this could be more efficient. This [bolt](https://stackoverflow.com/questions/49877898/stormcrawler-do-action-when-crawling-one-domain-finished) could tracks which domains are currently crawled and gives a signal when one domains has not been fetched after a specified time. Then the previous costly query could be executed which would be more efficient.
## Bugs
### Kibana not showing links which have the status `FETCHED`
Currently when you want to explore the Elasticsearch database Kibana will not show URLs with the status `FETCHED` even though they are in Elasticsearch. This could also be a bug from the configuration here as this apparently appears on other setups.
### Crawling intervals of 5-10 minutes
It seems that the crawler is crawling in intervals of 5 to 10 minutes. Possible reasons for this problem could be
- In the current setup not enough Memory for sufficient fetcher threads --> a lot of scheduling 
- Not enough shards from Elasticsearch which give sufficient URLs
- A bug in StormCrawler itself
- A low TTL value for URLs which have to be picked up and fetched see [here](https://github.com/DigitalPebble/storm-crawler/issues/396)
## Acknowledgements
This [project](https://hpi.de/naumann/teaching/bachelorprojekte/inventory-management.html) was done within a Bachelor project of the Hasso Plattner Insitute in cooperation with Idealo Internet GmbH 
Thanks to my Team 
- Leonardo Hübscher
- Dmitrii Zhamanakov
- Tom Schwarzburg 
- Daniel Lindner