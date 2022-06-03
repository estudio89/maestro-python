import 'package:frontend/models.dart';

class TodoRepository {
  List<Todo> _items = [];

  Future<List<Todo>> list() async {
    return Future.value(List.from(_items));
  }

  Future<Todo> create(Todo item) async {
    _items.add(item);
    return Future.value(item);
  }

  Future<Todo> update(Todo item) async {
    for (int i = 0; i < _items.length; i++) {
      Todo it = _items[i];
      if (it.id == item.id) {
        _items[i] = item;
        break;
      }
    }

    return Future.value(item);
  }

  Future<Todo> delete(Todo item) async {
    _items = _items.where((i) => i.id != item.id).toList();
    return Future.value(item);
  }
}

final todoRepository = TodoRepository();
