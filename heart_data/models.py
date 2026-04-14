from django.db import models

# Create your models here.
class DatasetUpload(models.Model):
    STATUS_CHOICES = [
        ("uploaded", "Uploaded"),
        ("processing", "Processing"),
        ("done", "Done"),
        ("failed", "Failed"),
    ]

    name = models.CharField(max_length=255)
    file = models.FileField(upload_to="datasets/")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="uploaded")
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name