import argparse
import io
import os.path as osp
import pprint
import sys

import barecat
import msgpack_numpy
import simplepyutils as spu
from PIL import Image
from PIL.ImageQt import ImageQt
from PyQt5.QtCore import QAbstractItemModel, QModelIndex, Qt, pyqtSlot
from PyQt5.QtGui import QFont, QImage, QPixmap, QStandardItem, QStandardItemModel
from PyQt5.QtWidgets import QAbstractItemView, QApplication, QHBoxLayout, QHeaderView, QLabel, \
    QScrollArea, QSplitter, QTableView, QTreeView, QWidget


def main():
    app = QApplication(sys.argv)
    parser = argparse.ArgumentParser(description='View images stored in a barecat archive.')
    parser.add_argument('path', type=str, help='path to load from')
    args = parser.parse_args()
    viewer = BareCatViewer(args.path)
    viewer.show()
    sys.exit(app.exec_())


class BareCatViewer(QWidget):
    def __init__(self, path):
        super().__init__()
        self.file_reader = barecat.Reader(path)
        self.barecat_path = path
        self.tree = QTreeView()

        self.file_table = QTableView()
        self.file_table.verticalHeader().setVisible(False)
        self.file_table.verticalHeader().setDefaultSectionSize(20)
        self.file_table.setShowGrid(False)

        self.file_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.file_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.file_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        model = QStandardItemModel()
        model.setHorizontalHeaderLabels(['Name', 'Size'])
        self.file_table.setModel(model)
        self.file_table.selectionModel().selectionChanged.connect(self.show_file)
        self.file_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.file_table.horizontalHeader().setStyleSheet(
            "QHeaderView::section {font-weight: normal; text-align: left;}")

        self.image_label = QLabel()
        self.image_label.setWordWrap(True)
        font = QFont("Courier New")  # Replace with the desired monospace font
        self.image_label.setFont(font)

        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setWidget(self.image_label)

        splitter = QSplitter()
        splitter.addWidget(self.tree)
        splitter.addWidget(self.file_table)
        splitter.addWidget(self.scroll_area)
        splitter.setSizes([650, 650, 1000])

        layout = QHBoxLayout()
        layout.addWidget(splitter)
        self.setLayout(layout)

        self.resize(2400, 800)

        self.fill_tree()
        self.tree.selectionModel().selectionChanged.connect(self.update_file_table)
        self.tree.activated.connect(self.expand_tree_item)
        self.tree.doubleClicked.connect(self.expand_tree_item)

        root_index = self.tree.model().index(0, 0)
        self.tree.setCurrentIndex(root_index)

    def fill_tree(self):
        root_item = TreeItem(self.file_reader)
        size, count, has_subdirs, has_files = self.file_reader.index.get_dir_info('')
        item = TreeItem(
            self.file_reader, path='', size=size, count=count, has_subdirs=has_subdirs,
            parent=root_item)
        root_item.children.append(item)
        self.model = LazyItemModel(root_item)
        self.tree.setModel(self.model)

        root_index = self.tree.model().index(0, 0)
        self.tree.expand(root_index)  # Expand the root item by default
        self.tree.setColumnWidth(0, 400)
        self.tree.setColumnWidth(1, 70)
        self.tree.setColumnWidth(2, 70)

    @pyqtSlot(QModelIndex)
    def expand_tree_item(self, index):
        if self.tree.isExpanded(index):
            self.tree.collapse(index)
        else:
            self.tree.expand(index)
    def update_file_table(self, selected, deselected):
        indexes = selected.indexes()
        if not indexes:
            return

        index = indexes[0]  # Get the first selected index
        item = index.internalPointer()

        model = self.file_table.model()
        model.removeRows(0, model.rowCount())
        files_and_sizes = self.file_reader.index.get_files_with_size(item.path)
        files_and_sizes = sorted(files_and_sizes, key=lambda x: spu.natural_sort_key(x[0]))
        for file, size in files_and_sizes:
            file_item = QStandardItem(osp.basename(file))
            file_item.setData(file, Qt.UserRole)  # Store the full path as user data
            model.appendRow([file_item, QStandardItem(format_size(size))])

        if len(files_and_sizes) > 0:
            first_file_index = self.file_table.model().index(0, 0)
            self.file_table.setCurrentIndex(first_file_index)
    def show_file(self, selected, deselected):
        indexes = selected.indexes()
        if not indexes:
            return

        index = indexes[0]
        path = self.file_table.model().item(index.row(), 0).data(Qt.UserRole)
        content = self.file_reader[path]
        extension = osp.splitext(path)[1].lower()
        if extension in ('.jpg', '.jpeg', '.png', '.gif', '.bmp'):
            image = Image.open(io.BytesIO(content))
            max_width = self.image_label.width()
            max_height = self.image_label.height()
            image.thumbnail((max_width, max_height), Image.LANCZOS)
            qim = ImageQt(image).convertToFormat(QImage.Format_RGBA8888)
            pixmap = QPixmap.fromImage(qim)
            self.update_image_label(pixmap)
        elif extension == '.msgpack':
            pp = pprint.PrettyPrinter(indent=2, width=150, compact=True, sort_dicts=False)
            self.image_label.setText(pp.pformat(msgpack_numpy.unpackb(content)))
            self.image_label.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        else:
            self.image_label.setText(repr(content))
            self.image_label.setAlignment(Qt.AlignLeft | Qt.AlignTop)

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
            return ["Name", "Size", "Count"][section]
        return None

    def data(self, index, role):
        item = index.internalPointer()
        if role == Qt.DisplayRole:
            if index.column() == 0:
                if item.parent == self.root:
                    return '[root]'
                return osp.basename(item.path)
            elif index.column() == 1:
                return format_size(item.size)
            elif index.column() == 2:
                return format_count(item.count)
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
        return index.internalPointer().has_subdirs


class TreeItem:
    def __init__(self, file_reader, path='', size=0, count=0, has_subdirs=True, parent=None):
        self.file_reader = file_reader

        self.path = path
        self.parent = parent
        self.children = []

        self.size = size
        self.count = count
        self.has_subdirs = has_subdirs
        self.fetched = False

    def fetch_more(self):
        if self.fetched:
            return
        subdir_infos = self.file_reader.index.get_subdir_infos(self.path)
        subdir_infos = sorted(subdir_infos, key=lambda x: spu.natural_sort_key(x[0]))
        for dir, size, count, has_subdirs, has_files in subdir_infos:
            self.children.append(TreeItem(
                self.file_reader, path=dir, size=size, count=count, has_subdirs=has_subdirs,
                parent=self))

        self.fetched = True

    @property
    def row(self):
        return self.parent.children.index(self) if self.parent else 0


def format_size(size):
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    index = 0
    while size >= 1024:
        index += 1
        size /= 1024
    return f'{size:.2f} {units[index]}'


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
