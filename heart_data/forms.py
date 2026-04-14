from django import forms
from .models import DatasetUpload

class DatasetUploadForm(forms.ModelForm):
    class Meta:
        model = DatasetUpload
        fields = ["name", "file"]