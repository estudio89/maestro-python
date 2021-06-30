from django.http import HttpResponse
from django.apps import apps


def update_item_view(request, item_id):
    Item = apps.get_model("my_app", "Item")
    item = Item.objects.get(id=item_id)
    item.version += "2"
    item.save()

    return HttpResponse()
