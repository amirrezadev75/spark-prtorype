from django.urls import path
from . import views

urlpatterns = [
    path("upload/", views.upload_dataset, name="upload_dataset"),
    path("", views.dataset_list, name="dataset_list"),
    path("dataset/<int:pk>/", views.dataset_detail, name="dataset_detail"),
]