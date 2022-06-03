import 'package:frontend/models.dart';
import 'package:cloud_firestore/cloud_firestore.dart';

class TodoRepository {
  FirebaseFirestore _firestore = FirebaseFirestore.instance;

  Future<List<Todo>> list() async {
    final QuerySnapshot<Map<String, dynamic>> snapshot =
        await _firestore.collection('todos').orderBy("date").get();
    return snapshot.docs.map((DocumentSnapshot<Map<String, dynamic>> doc) {
      final Map<String, dynamic> data = doc.data()!;
      return Todo(
          id: doc.id,
          text: data["text"],
          done: data["done"],
          date: data["date"]);
    }).toList();
  }

  Future<Todo> create(Todo item) async {
    await _firestore
        .collection('todos')
        .doc(item.id)
        .set({"text": item.text, "done": item.done, "date": item.date});
    return Future.value(item);
  }

  Future<Todo> update(Todo item) async {
    return create(item);
  }

  Future<Todo> delete(Todo item) async {
    await _firestore.collection('todos').doc(item.id).delete();
    return item;
  }
}

final todoRepository = TodoRepository();
