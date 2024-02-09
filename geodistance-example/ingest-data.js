const fs = require('fs');
const { Client } = require('@opensearch-project/opensearch');
const { defaultProvider } = require('@aws-sdk/credential-provider-node');
const { AwsSigv4Signer } = require('@opensearch-project/opensearch/aws');

const region = process.env.AWS_DEFAULT_REGION; // Ensure this is correctly set in your environment
const endpoint = process.env.OPENSEARCH_ENDPOINT; // Your OpenSearch Serverless endpoint
const indexName = 'geo-cities'; // The index name for your documents

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
      })),
    }),
  });
};

const createIndex = async (client) => {
  await client.indices.create({
    index: indexName,
    body: {
      mappings: {
        properties: {
          city: { type: 'text' },
          state: { type: 'text' },
          location: { type: 'geo_point' },
        },
      },
    },
  });
};

const indexDocument = async (client, document, id) => {
  try {
    const response = await client.index({
      index: indexName,
      id: String(id), // Convert ID to string to ensure compatibility
      body: document,
    });
    console.log(`Document indexed successfully: ID=${id}`, response);
  } catch (error) {
    console.error(`Error indexing document ID=${id}:`, error);
  }
};

const ingestData = async () => {
  const client = buildClient();
  await createIndex(client); // Ensure the index is created with the correct mappings before ingesting data
  for (const [index, item] of data.entries()) {
    await indexDocument(client, item, index); // Using array index as the document ID for simplicity
  }
  console.log('Data ingestion completed successfully.');
};

ingestData().catch(console.error);
