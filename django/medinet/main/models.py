from django.contrib.auth.models import AbstractUser, Permission, Group
from django.db import models


class CustomUser(AbstractUser):
    # Добавляем дополнительные поля, если нужно
    phone_number = models.CharField(max_length=20, blank=True)

    class Meta:
        permissions = [
            ("can_view_admin_page", "Can view admin page"),
        ]