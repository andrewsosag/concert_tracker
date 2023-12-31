const admin = require('firebase-admin');
const serviceAccount = require('../concert-price-tracker-5921b-firebase-adminsdk-ujei5-975fd322a8.json');

// Initialize Firebase Admin SDK
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

async function deleteCollection(collectionPath) {
  const collectionRef = db.collection(collectionPath);
  const snapshot = await collectionRef.get();

  const batchSize = snapshot.size;
  if (batchSize === 0) {
    console.log('No documents to delete.');
    return;
  }

  // Delete documents in a batch
  const batch = db.batch();
  snapshot.docs.forEach(doc => {
    batch.delete(doc.ref);
  });
  await batch.commit();

  console.log(`Deleted ${batchSize} documents in the collection ${collectionPath}`);
}

// Call the function for each collection
deleteCollection('events').catch(console.error);
deleteCollection('event_prices').catch(console.error);
