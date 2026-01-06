from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required, permission_required
from django.contrib.auth.models import Group, Permission
from django.contrib import messages
from .models import CustomUser
from .forms import CustomUserCreationForm  # Импортируем нашу форму


def home(request):
    return render(request, 'home.html')


def public_page(request):
    return render(request, 'public_page.html')


@login_required
@permission_required('main.can_view_admin_page', raise_exception=True)
def admin_page(request):
    # Получаем или создаем группу администраторов
    admin_group, created = Group.objects.get_or_create(name='Администраторы')

    # Добавляем разрешение в группу, если его там нет
    can_view_admin_page = Permission.objects.get(codename='can_view_admin_page')
    if can_view_admin_page not in admin_group.permissions.all():
        admin_group.permissions.add(can_view_admin_page)

    if request.method == 'POST':
        form = CustomUserCreationForm(request.POST)  # Используем нашу форму
        if form.is_valid():
            user = form.save()

            # Добавляем пользователя в группу администраторов
            if 'make_admin' in request.POST and request.POST['make_admin'] == 'on':
                admin_group.user_set.add(user)
                messages.success(request, f'Пользователь {user.username} создан и добавлен в администраторы')
            else:
                messages.success(request, f'Пользователь {user.username} создан')

            return redirect('admin_page')
    else:
        form = CustomUserCreationForm()  # Используем нашу форму

    # Получаем всех пользователей
    users = CustomUser.objects.all()

    context = {
        'form': form,
        'users': users,
        'admin_group': admin_group,
    }
    return render(request, 'admin_page.html', context)