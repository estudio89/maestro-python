import 'package:cloud_firestore/cloud_firestore.dart';

class Todo {
  final String id;
  final String text;
  final bool done;
  final Timestamp date;

  Todo({
    required this.id,
    required this.text,
    required this.done,
    required this.date,
  });

  Todo copyWith({
    String? id,
    String? text,
    bool? done,
    Timestamp? date,
  }) {
    return Todo(
      id: id ?? this.id,
      text: text ?? this.text,
      done: done ?? this.done,
      date: date ?? this.date,
    );
  }

  @override
  String toString() => 'Todo(id: $id, text: $text, done: $done, date: $date)';

  @override
  bool operator ==(Object other) {
    if (identical(this, other)) return true;

    return other is Todo && other.id == id;
  }

  @override
  int get hashCode =>
      id.hashCode ^ text.hashCode ^ done.hashCode ^ date.hashCode;
}
