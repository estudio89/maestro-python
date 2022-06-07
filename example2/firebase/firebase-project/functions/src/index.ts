import * as admin from "firebase-admin";
import { setupCommitQueue, FirestoreAppItemSerializer } from "maestro";
import { QueuedOperation } from "maestro/dist/backends/firestore/collections";

admin.initializeApp();

export const commitQueue = setupCommitQueue(
	new FirestoreAppItemSerializer(),
	() => {
		// return fetch(
		// 	"http://10.222.0.5:1212" + "/api/sync/firestore/",
		// 	{
		// 		method: "POST",
		// 		headers: {
		// 			Authorization: functions.config().django.sync_secret,
		// 		},
		// 	}
		// );
		return new Promise<void>((resolve, reject) => {
			resolve();
		});
	},
	async (queuedOperation: QueuedOperation, error: any) => {
		console.log(
			`Error while commiting change with id ${queuedOperation.id}: ${error} | ${error.stack}`
		);
	},
	"firestore",
	admin.firestore()
);