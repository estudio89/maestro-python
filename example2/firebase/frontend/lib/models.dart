class Todo {
  final String id;
  final String text;
  final bool done;

  Todo({
    required this.id,
    required this.text,
    required this.done,
  });

  Todo copyWith({
    String? id,
    String? text,
    bool? done,
  }) {
    return Todo(
      id: id ?? this.id,
      text: text ?? this.text,
      done: done ?? this.done,
    );
  }

  @override
  String toString() => 'Todo(id: $id, text: $text, done: $done)';

  @override
  bool operator ==(Object other) {
    if (identical(this, other)) return true;

    return other is Todo && other.id == id;
  }

  @override
  int get hashCode => id.hashCode ^ text.hashCode ^ done.hashCode;
}
