from typing import List, Optional, Union, Dict, Any
from sqlalchemy.orm import Session

from app.models.file import File
from app.schemas.file import FileCreate, FileUpdate


def get(db: Session, file_id: int) -> Optional[File]:
    """Получение файла по ID"""
    return db.query(File).filter(File.id == file_id).first()


def get_multi(
    db: Session, *, skip: int = 0, limit: int = 100
) -> List[File]:
    """Получение списка файлов"""
    query = db.query(File)
    return query.offset(skip).limit(limit).all()


def create(db: Session, *, obj_in: FileCreate) -> File:
    """Создание нового файла"""
    db_obj = File(
        filename=obj_in.filename,
        original_filename=obj_in.original_filename,
        file_path=obj_in.file_path,
        file_size=obj_in.file_size,
        file_type=obj_in.file_type,
        description=obj_in.description,
        is_active=obj_in.is_active,
    )
    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)
    return db_obj


def update(
    db: Session,
    *,
    db_obj: File,
    obj_in: Union[FileUpdate, Dict[str, Any]]
) -> File:
    """Обновление файла"""
    if isinstance(obj_in, dict):
        update_data = obj_in
    else:
        update_data = obj_in.dict(exclude_unset=True)
    
    for field in update_data:
        setattr(db_obj, field, update_data[field])
    
    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)
    return db_obj


def remove(db: Session, *, file_id: int) -> File:
    """Удаление файла"""
    obj = db.query(File).get(file_id)
    db.delete(obj)
    db.commit()
    return obj 