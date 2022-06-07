import 'package:frontend/models.dart';
import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:uuid/uuid.dart';

enum Operation { INSERT, UPDATE, DELETE }

class TodoRepository {
  FirebaseFirestore _firestore = FirebaseFirestore.instance;

  Future<void> _commitOperation(Todo item, Operation operation) async {
    String operationStr = operation.toString().split('.').last;
    Map<String, dynamic> data = {
      "text": item.text,
      "done": item.done,
      "date": item.date
    };

    Uuid uuid = new Uuid();
    final String itemChangeId = uuid.v4();
    _firestore.collection("maestro__commit_queue").doc(itemChangeId).set({
      "id": itemChangeId,
      "item_id": item.id,
      "collection_name": "core_todo",
      "operation": operationStr,
      "data": data,
      "timestamp": FieldValue.serverTimestamp(),
      "status": "pending"
    });
  }

  Future<List<Todo>> list() async {
    final QuerySnapshot<Map<String, dynamic>> snapshot =
        await _firestore.collection('core_todo').orderBy("date").get();
    return snapshot.docs.map((DocumentSnapshot<Map<String, dynamic>> doc) {
      final Map<String, dynamic> data = doc.data()!;
      return Todo(
          id: doc.id,
          text: data["text"],
          done: data["done"],
          date: (data["date"] as Timestamp).toDate());
    }).toList();
  }

  Future<Todo> create(Todo item) async {
    await _firestore
        .collection('core_todo')
        .doc(item.id)
        .set({"text": item.text, "done": item.done, "date": item.date});

    await _commitOperation(item, Operation.INSERT);
    return Future.value(item);
  }

  Future<Todo> update(Todo item) async {
    await create(item);
    await _commitOperation(item, Operation.UPDATE);
    return item;
  }

  Future<Todo> delete(Todo item) async {
    await _firestore.collection('core_todo').doc(item.id).delete();
    await _commitOperation(item, Operation.DELETE);
    return item;
  }
}

final todoRepository = TodoRepository();
