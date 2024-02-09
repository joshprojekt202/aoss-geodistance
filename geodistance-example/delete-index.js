const { Client } = require('@opensearch-project/opensearch');
const { defaultProvider } = require('@aws-sdk/credential-provider-node');
const { AwsSigv4Signer } = require('@opensearch-project/opensearch/aws');

const region = process.env.AWS_DEFAULT_REGION; // Ensure this is correctly set in your environment
const endpoint = process.env.OPENSEARCH_ENDPOINT; // Your OpenSearch Serverless endpoint
const indexName = 'geo-cities-collection'; // The name of the index you want to delete

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

const deleteIndex = async (client, indexName) => {
  try {
    const response = await client.indices.delete({
      index: indexName,
    });
    console.log(`Index deleted successfully: ${indexName}`, response);
  } catch (error) {
    console.error(`Error deleting index ${indexName}:`, error);
  }
};

const run = async () => {
  const client = buildClient();
  await deleteIndex(client, indexName);
};

run().catch(console.error);
