from django.shortcuts import get_object_or_404, render, redirect
from .forms import DatasetUploadForm
from .models import DatasetUpload
from .spark_jobs.analyze_heart_data import run_pipeline

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

def dataset_detail(request, pk):
    dataset = get_object_or_404(DatasetUpload, pk=pk)

    analysis_results = None
    error_message = None

    try:
        file_path = dataset.file.path
        results = run_pipeline(file_path)

        # normalize keys for template friendliness
        def normalize_list(rows):
            return [
                {k.lower().replace(" ", "_"): v for k, v in row.items()}
                for row in rows
            ]

        def normalize_dict(d):
            return {k.lower().replace(" ", "_"): v for k, v in d.items()}

        analysis_results = {
            "summary": results["summary"],
            "numeric_summary": normalize_list(results["numeric_summary"]),
            "null_counts": normalize_dict(results["null_counts"]),
            "by_gender": normalize_list(results["by_gender"]),
            "by_smoking": normalize_list(results["by_smoking"]),
            "by_heart_status": normalize_list(results["by_heart_status"]),
        }

    except Exception as e:
        error_message = str(e)

    return render(
        request,
        "heart_data/dataset_detail.html",
        {
            "dataset": dataset,
            "analysis_results": analysis_results,
            "error_message": error_message,
        },
    )