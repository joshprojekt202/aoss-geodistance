
const fs = require('fs');
const { Client } = require('@opensearch-project/opensearch');
const { defaultProvider } = require('@aws-sdk/credential-provider-node');
const { AwsSigv4Signer } = require('@opensearch-project/opensearch/aws');

const region = process.env.AWS_DEFAULT_REGION; // Ensure this is correctly set in your environment
const endpoint = process.env.OPENSEARCH_ENDPOINT; // Your OpenSearch Serverless endpoint
const indexName = 'geo-cities'; // The target index for your documents

// Load mock data from cities-data.json
const dataPath = './cities-data.json';
const data = JSON.parse(fs.readFileSync(dataPath, 'utf8'));

const buildClient = () => {
  const credentialsProvider = defaultProvider(); // Initialize the AWS credentials provider
  return new Client({
    node: endpoint,
    ...AwsSigv4Signer({
      region: region,
      service: 'aoss', // Use 'aoss' for OpenSearch Serverless
      getCredentials: () => credentialsProvider().then(credentials => ({
        accessKeyId: credentials.accessKeyId,
        secretAccessKey: credentials.secretAccessKey,
        sessionToken: credentials.sessionToken, // Added support for temporary credentials
      })),
    }),
  });
};

const prettyPrint = (query, results) => {
  console.log('\n================================================================================');
  console.log(`Querying for documents within ${query.distance} miles of location (${query.location.lat}, ${query.location.lon}):`);
  console.log('================================================================================');
  if(results.length > 0){
    results.forEach((result, index) => {
      console.log('\n----------------------------------------');
      console.log(`Result ${index + 1}:`);
      console.log(`City: ${result._source.city} (${result._source.state})`);
      console.log(`Location: (${result._source.location.lat}, ${result._source.location.lon})`);
      console.log('----------------------------------------');
    });
  } else {
    console.log("\nNo results found.");
  }
};

const searchByGeoDistance = async (client, location, distance) => {
  const query = {
    query: {
      bool: {
        must: {
          match_all: {}
        },
        filter: {
          geo_distance: {
            distance: `${distance}mi`, // Specify distance in miles
            'location': location
          }
        }
      }
    }
  };

  try {
    const response = await client.search({
      index: indexName,
      body: query
    });
    prettyPrint({ location, distance }, response.body.hits.hits);
  } catch (error) {
    console.error(`\nError performing geo-distance search:`, error);
  }
};

const runQueries = async () => {
  const client = buildClient();
  await searchByGeoDistance(client, { lat: 40.0150, lon: -105.2705 }, '50');
  await searchByGeoDistance(client, { lat: 35.5951, lon: -82.5515 }, '100');
  await searchByGeoDistance(client, { lat: 44.9778, lon: -93.2650 }, '200');
  await searchByGeoDistance(client, { lat: 34.0522, lon: -118.2437 }, '300');
  await searchByGeoDistance(client, { lat: 25.7617, lon: -80.1918 }, '500');
};

runQueries().catch(console.error);