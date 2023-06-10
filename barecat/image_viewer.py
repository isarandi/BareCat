import argparse
import io
import os.path as osp
import sys

import barecat
from PIL import Image
from PIL.ImageQt import ImageQt
from PyQt5.QtCore import QAbstractItemModel, QModelIndex, Qt, pyqtSlot
from PyQt5.QtGui import QImage, QPixmap
from PyQt5.QtWidgets import QApplication, QHBoxLayout, QLabel, QSplitter, QTreeView, QWidget


def main():
    app = QApplication(sys.argv)
    parser = argparse.ArgumentParser(description='View images stored in a barecat archive.')
    parser.add_argument('path', type=str, help='path to load from')
    args = parser.parse_args()
    viewer = BareCatImageViewer(args.path)
    viewer.show()
    sys.exit(app.exec_())


class BareCatImageViewer(QWidget):
    def __init__(self, path):
        super().__init__()
        self.file_reader = barecat.Reader(path)
        self.barecat_path = path
        self.tree = QTreeView()
        self.image_label = QLabel()

        splitter = QSplitter()
        splitter.addWidget(self.tree)
        splitter.addWidget(self.image_label)
        splitter.setSizes([650, 1000])

        layout = QHBoxLayout()
        layout.addWidget(splitter)
        self.setLayout(layout)

        self.resize(1600, 800)

        self.tree.clicked.connect(self.show_image)
        self.fill_tree()

    def fill_tree(self):
        root_item = TreeItem(
            self.file_reader, path='', parent=None, is_file=False, dirs=[''], files=[])
        dirs, files = self.file_reader.listdir('')
        item = TreeItem(
            self.file_reader, path='', parent=root_item, is_file=False, dirs=dirs,
            files=files)
        root_item.children.append(item)
        self.model = LazyItemModel(root_item)
        self.tree.setModel(self.model)

        root_index = self.tree.model().index(0, 0)
        self.tree.expand(root_index)  # Expand the root item by default
        self.tree.setColumnWidth(0, 400)
        self.tree.setColumnWidth(1, 70)
        self.tree.setColumnWidth(2, 70)

    def show_image(self, index):
        item = index.internalPointer()
        if not item.is_file:
            return

        with self.file_reader.open(item.path) as file_in_archive:
            image = Image.open(file_in_archive)

        max_width = self.image_label.width()
        max_height = self.image_label.height()
        image.thumbnail((max_width, max_height), Image.LANCZOS)
        qim = ImageQt(image).convertToFormat(QImage.Format_RGBA8888)
        pixmap = QPixmap.fromImage(qim)
        self.update_image_label(pixmap)

    @pyqtSlot(int, int)
    def update_image_size(self, pos, index):
        # Scale the image to fill the available space
        pixmap = self.image_label.pixmap()
        if pixmap is not None:
            pixmap = pixmap.scaled(
                self.image_label.size(), Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation)
            self.update_image_label(pixmap)

    def update_image_label(self, pixmap):
        self.image_label.setPixmap(pixmap)
        self.image_label.setAlignment(Qt.AlignCenter)


class LazyItemModel(QAbstractItemModel):
    def __init__(self, root):
        super().__init__()
        self.root = root

    def index(self, row, column, parent=QModelIndex()):
        if not self.hasIndex(row, column, parent):
            return QModelIndex()
        parent_item = self.root if not parent.isValid() else parent.internalPointer()
        return (
            self.createIndex(row, column, parent_item.children[row])
            if row < len(parent_item.children)
            else QModelIndex())

    def parent(self, index):
        if not index.isValid():
            return QModelIndex()
        parent_item = index.internalPointer().parent
        return self.createIndex(parent_item.row, 0, parent_item) if parent_item else QModelIndex()

    def rowCount(self, parent=QModelIndex()):
        parent_item = self.root if not parent.isValid() else parent.internalPointer()
        return len(parent_item.children)

    def columnCount(self, parent=QModelIndex()):
        return 3  # Name, Size, Count

    def headerData(self, section, orientation, role):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            return ["Name", "Size", "File Count"][section]
        return None

    def data(self, index, role):
        item = index.internalPointer()
        if role == Qt.DisplayRole:
            if index.column() == 0:
                if item.parent == self.root:
                    return '[root]'
                return osp.basename(item.path)
            elif index.column() == 1:
                return format_size(item.subtree_size)
            elif index.column() == 2:
                if not item.is_file:
                    return format_count(item.subtree_file_count)
        elif role == Qt.TextAlignmentRole:
            if index.column() in [1, 2]:
                return Qt.AlignRight
        return None

    def canFetchMore(self, index):
        if not index.isValid():
            return False
        return not index.internalPointer().fetched

    def fetchMore(self, index):
        item = index.internalPointer()
        item.fetch_more()
        self.beginInsertRows(index, 0, len(item.children) - 1)
        self.endInsertRows()

    def hasChildren(self, index=QModelIndex()):
        if not index.isValid():
            return True
        return not index.internalPointer().is_leaf


class TreeItem:
    def __init__(self, file_reader, path='', parent=None, is_file=False, dirs=(), files=()):
        self.file_reader = file_reader

        self.path = path
        self.parent = parent
        self.children = []

        self.is_file = is_file
        self.dirs = dirs
        self.files = files

        if self.is_file:
            self.subtree_size = self.file_reader.get_file_size(self.path)
            self.subtree_file_count = 1
        else:
            self.subtree_size = self.file_reader.get_subtree_size(self.path)
            self.subtree_file_count = self.file_reader.get_subtree_file_count(self.path)

        self.fetched = False

    def fetch_more(self):
        for direc in self.dirs:
            dirpath = osp.join(self.path, direc)
            subdirs, subfiles = self.file_reader.listdir(dirpath)
            item = TreeItem(self.file_reader, dirpath, self, dirs=subdirs, files=subfiles)
            self.children.append(item)
        for file in self.files:
            filepath = osp.join(self.path, file)
            item = TreeItem(self.file_reader, filepath, self, is_file=True)
            self.children.append(item)

        self.fetched = True

    @property
    def is_leaf(self):
        return self.is_file or len(self.dirs) + len(self.files) == 0

    @property
    def row(self):
        return self.parent.children.index(self) if self.parent else 0


def format_size(size):
    # Convert size in bytes to a more human-readable format
    units = [' bytes', ' KB', ' MB', ' GB', ' TB']
    unit_index = 0
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    return f'{size:.1f}{units[unit_index]}'


def format_count(size):
    units = ['', ' K', ' M', ' B']
    unit_index = 0
    while size >= 1000 and unit_index < len(units) - 1:
        size /= 1000
        unit_index += 1
    if unit_index == 0:
        return str(size)
    return f'{size:.1f}{units[unit_index]}'


if __name__ == '__main__':
    main()
