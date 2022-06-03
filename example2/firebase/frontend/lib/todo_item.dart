import 'package:flutter/material.dart';
import 'package:frontend/utils.dart';

import 'models.dart';

class TodoItem extends StatefulWidget {
  final Todo item;
  final void Function(Todo item) onChange;
  final void Function(Todo item) onDelete;

  const TodoItem(
      {required this.item,
      required this.onChange,
      required this.onDelete,
      Key? key})
      : super(key: key);

  @override
  State<TodoItem> createState() => _TodoItemState();
}

class _TodoItemState extends State<TodoItem> {
  late Todo _item;
  final TextEditingController _controller = TextEditingController();
  final Debouncer _debouncer = Debouncer(milliseconds: 300);
  bool _showDelete = false;

  @override
  initState() {
    super.initState();
    this._item = widget.item;
    _controller.text = _item.text;
    _controller.addListener(() {
      _debouncer.run(() {
        _item = _item.copyWith(text: _controller.text);
        widget.onChange(_item);
      });
    });
  }

  @override
  Widget build(BuildContext context) {
    return MouseRegion(
      onHover: (event) {
        setState(() {
          _showDelete = true;
        });
      },
      onExit: (event) {
        setState(() {
          _showDelete = false;
        });
      },
      child: Container(
        padding: const EdgeInsets.all(10),
        margin: const EdgeInsets.only(bottom: 15),
        decoration: BoxDecoration(
            color: Colors.white,
            borderRadius: BorderRadius.circular(10),
            boxShadow: [
              BoxShadow(
                  offset: const Offset(0, 1),
                  blurRadius: 9.0,
                  spreadRadius: 1.0,
                  color: Colors.black.withAlpha(40))
            ]),
        child: Row(
          children: [
            Checkbox(
                value: _item.done,
                onChanged: (bool? checked) {
                  setState(() {
                    _item = _item.copyWith(done: checked);
                    _debouncer.run(() {
                      widget.onChange(_item);
                    });
                  });
                }),
            Expanded(
                child: TextField(
              controller: _controller,
              style: const TextStyle(fontSize: 18),
              decoration: InputDecoration(
                enabledBorder: OutlineInputBorder(
                    borderRadius: BorderRadius.circular(10),
                    borderSide: const BorderSide(color: Colors.white)),
                filled: true,
                fillColor: Colors.white,
                focusedBorder: OutlineInputBorder(
                    borderRadius: BorderRadius.circular(10),
                    borderSide: BorderSide(
                        color: const Color(0xFF4254b3).withAlpha(175),
                        width: 2)),
              ),
            )),
            AnimatedOpacity(
              opacity: _showDelete ? 1 : 0,
              duration: const Duration(),
              child: IconButton(
                onPressed: () {
                  widget.onDelete(_item);
                },
                icon: Icon(Icons.delete, color: Colors.red[200]),
              ),
            )
          ],
        ),
      ),
    );
  }

  @override
  void dispose() {
    super.dispose();
    _controller.dispose();
    _debouncer.dispose();
  }
}
