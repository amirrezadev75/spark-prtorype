from django.shortcuts import render, redirect
from .forms import DatasetUploadForm
from .models import DatasetUpload

def upload_dataset(request):
    if request.method == "POST":
        form = DatasetUploadForm(request.POST, request.FILES)
        if form.is_valid():
            form.save()
            return redirect("dataset_list")
    else:
        form = DatasetUploadForm()

    return render(request, "heart_data/upload.html", {"form": form})


def dataset_list(request):
    datasets = DatasetUpload.objects.order_by("-uploaded_at")
    return render(request, "heart_data/dataset_list.html", {"datasets": datasets})