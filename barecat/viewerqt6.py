import argparse
import os
import os.path as osp
import pprint
import re
import shutil
import sys
from typing import List

import msgpack_numpy
from PyQt6.QtCore import QBuffer, QByteArray, QMimeData, QModelIndex, Qt, pyqtSlot
from PyQt6.QtGui import QClipboard, QFont, QFontMetrics, QImageReader, QPixmap, QStandardItem, \
    QStandardItemModel
from PyQt6.QtWidgets import (QAbstractItemView, QApplication, QFileDialog, QHBoxLayout,
                             QHeaderView, \
                             QLabel, QMenu, QScrollArea, QSplitter, QStyleFactory, QTableView,
                             QTreeView, QVBoxLayout, \
                             QWidget)

import barecat
from barecat.common import BarecatDirInfo, BarecatFileInfo


def main():
    app = QApplication(sys.argv)
    app.setStyle(QStyleFactory.create(QApplication.style().objectName()))

    parser = argparse.ArgumentParser(description='View images stored in a barecat archive.')
    parser.add_argument('path', type=str, help='path to load from')
    args = parser.parse_args()
    viewer = BarecatViewer(args.path)
    viewer.show()
    sys.exit(app.exec())


class BarecatViewer(QWidget):
    def __init__(self, path):
        super().__init__()
        self.file_reader = barecat.Barecat(path)
        self.barecat_path = path
        self.tree = QTreeView()
        self.tree.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        self.file_table = self.create_file_table()
        self.content_viewer = ContentViewer()
        self.content_viewer.label.setWordWrap(True)
        font = QFont("Courier New")  # Replace with the desired monospace font
        self.content_viewer.label.setFont(font)

        splitter = QSplitter()
        splitter.addWidget(self.tree)
        splitter.addWidget(self.file_table)
        splitter.addWidget(self.content_viewer)
        splitter.setSizes([650, 650, 1000])
        layout = QHBoxLayout()
        layout.addWidget(splitter)
        self.setLayout(layout)

        self.resize(2400, 800)

        self.fill_tree()
        self.tree.selectionModel().selectionChanged.connect(self.update_file_table)
        self.tree.activated.connect(self.expand_tree_item)
        self.tree.doubleClicked.connect(self.expand_tree_item)
        self.tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tree.customContextMenuRequested.connect(self.show_tree_context_menu)

        root_index = self.tree.model().index(0, 0)
        self.tree.setCurrentIndex(root_index)

    def create_file_table(self):
        ft = QTableView()
        ft.verticalHeader().setVisible(False)
        ft.verticalHeader().setDefaultSectionSize(20)
        ft.setShowGrid(False)
        ft.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        ft.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        ft.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        model = QStandardItemModel()
        model.setHorizontalHeaderLabels(['Name', 'Size'])
        ft.setModel(model)
        ft.selectionModel().selectionChanged.connect(self.show_selected_file)
        ft.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        ft.horizontalHeader().setStyleSheet(
            "QHeaderView::section {font-weight: normal; text-align: left;}")
        ft.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        ft.customContextMenuRequested.connect(self.show_file_table_context_menu)
        return ft

    def fill_tree(self):
        root_item = TreeItem(self.file_reader)
        dinfo: BarecatDirInfo = self.file_reader.index.lookup_dir('')
        item = TreeItem(
            self.file_reader, path='', size=dinfo.size_tree, count=dinfo.num_files_tree,
            has_subdirs=dinfo.num_subdirs > 0, parent=root_item)
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
        finfos: List[BarecatFileInfo] = self.file_reader.index.list_direct_fileinfos(item.path)
        finfos = sorted(finfos, key=lambda x: natural_sort_key(x.path))
        for finfo in finfos:
            file_item = QStandardItem(osp.basename(finfo.path))
            file_item.setData(finfo, Qt.ItemDataRole.UserRole)  # Store the fileinfo as user data
            model.appendRow([file_item, QStandardItem(format_size(finfo.size))])

        if len(finfos) > 0:
            first_file_index = self.file_table.model().index(0, 0)
            self.file_table.setCurrentIndex(first_file_index)
        else:
            for dinfo, subdinfos, finfos in self.file_reader.index.walk_infos(item.path):
                finfo = next(iter(finfos), None)
                if finfo is not None:
                    self.show_file(finfo)
                    break

    def show_selected_file(self, selected, deselected):
        indexes = selected.indexes()
        if not indexes:
            return
        path = self.file_table.model().item(indexes[0].row(), 0).data(Qt.ItemDataRole.UserRole)
        self.show_file(path)

    def show_file(self, finfo):
        content = self.file_reader.read(finfo)
        extension = osp.splitext(finfo.path)[1].lower()
        if extension in ('.jpg', '.jpeg', '.png', '.gif', '.bmp'):
            byte_array = QByteArray(content)
            buffer = QBuffer(byte_array)
            imageReader = QImageReader()
            imageReader.setDecideFormatFromContent(True)
            imageReader.setQuality(100)
            imageReader.setDevice(buffer)
            qim = imageReader.read()

            if not qim.isNull():
                pixmap = QPixmap.fromImage(qim)
                self.content_viewer.setPixmap(pixmap)
        elif extension == '.msgpack':
            data = msgpack_numpy.unpackb(content)
            self.content_viewer.setText(data)
        else:
            self.content_viewer.setText(repr(content))

    def update_image_label(self, pixmap):
        self.content_viewer.setPixmap(pixmap)

    def show_file_table_context_menu(self, position):
        menu = QMenu()
        extract_action = menu.addAction("Extract file...")
        copy_path_action = menu.addAction("Copy path")

        action = menu.exec(self.file_table.viewport().mapToGlobal(position))

        if action == extract_action:
            indexes = self.file_table.selectionModel().selectedRows()
            if indexes:
                path_of_what_to_extract = self.file_table.model().item(indexes[0].row(), 0).data(
                    Qt.ItemDataRole.UserRole)
                default_filename = osp.basename(path_of_what_to_extract)
                target_filename, _ = QFileDialog.getSaveFileName(
                    self, "Select Target File", default_filename)
                if target_filename:
                    self.extract_file(path_of_what_to_extract, target_filename)
        elif action == copy_path_action:
            indexes = self.file_table.selectionModel().selectedRows()
            if indexes:
                path = self.file_table.model().item(indexes[0].row(), 0).data(
                    Qt.ItemDataRole.UserRole)
                clipboard = QApplication.clipboard()
                clipboard.setText(path)

    def show_tree_context_menu(self, position):
        menu = QMenu()
        extract_action = menu.addAction("Extract directory...")
        copy_path_action = menu.addAction("Copy path")

        action = menu.exec(self.tree.viewport().mapToGlobal(position))
        if action == extract_action:
            index = self.tree.indexAt(position)
            if index.isValid():
                if target_directory := QFileDialog.get(self, "Select Target Directory"):
                    self.extract_directory(index.internalPointer().path, target_directory)
        elif action == copy_path_action:
            index = self.tree.indexAt(position)
            if index.isValid():
                clipboard = QApplication.clipboard()
                clipboard.setText(index.internalPointer().path)

    def extract_file(self, path_of_what_to_extract, target_filename):
        with open(target_filename, 'wb') as f:
            shutil.copyfileobj(self.file_reader.open(path_of_what_to_extract), f)

    def extract_directory(self, dir_in_archive, target_directory):
        basename = osp.basename(dir_in_archive)
        for dinfo, _, finfos in self.file_reader.index.walk_infos():
            for finfo in finfos:
                target_path = osp.join(
                    target_directory, basename, osp.relpath(finfo.path, dir_in_archive))
                os.makedirs(osp.dirname(target_path), exist_ok=True)
                with open(target_path, 'wb') as f:
                    shutil.copyfileobj(self.file_reader.open(finfo.path), f)


class ContentViewer(QWidget):
    def __init__(self):
        super().__init__()
        self.label = QLabel()
        self.originalPixmap = None
        self.originalText = None  # New attribute to hold the original text
        self.scrollArea = QScrollArea(self)
        self.scrollArea.setWidgetResizable(True)
        self.scrollArea.setWidget(self.label)
        layout = QVBoxLayout(self)
        layout.addWidget(self.scrollArea)

        self.label.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.label.customContextMenuRequested.connect(self.show_context_menu)

    def setPixmap(self, pixmap):
        self.originalPixmap = pixmap
        self.originalText = None  # Reset the original text
        self.updateImage()

    def setText(self, original_data):
        self.originalText = original_data  # Store the original data
        self.originalPixmap = None  # Reset the pixmap
        self.updateText()

    def updateImage(self):
        if self.originalPixmap:
            availableSize = self.scrollArea.size()
            if self.originalPixmap.width() > availableSize.width() or self.originalPixmap.height(

            ) > availableSize.height():
                scaledPixmap = self.originalPixmap.scaled(availableSize,
                                                          Qt.AspectRatioMode.KeepAspectRatio,
                                                          Qt.TransformationMode.SmoothTransformation)
            else:
                scaledPixmap = self.originalPixmap
            self.label.setPixmap(scaledPixmap)
            self.label.setAlignment(Qt.AlignmentFlag.AlignCenter)

    def updateText(self):
        if self.originalText:
            # Calculate the maximum line width
            width_pixels = self.scrollArea.width()
            fm = QFontMetrics(self.label.font())
            average_char_width_pixels = fm.averageCharWidth()
            max_line_width = width_pixels // average_char_width_pixels

            # Pretty-print the text
            pp = pprint.PrettyPrinter(indent=2, width=max_line_width, compact=True,
                                      sort_dicts=False)
            formatted_text = pp.pformat(self.originalText)
            self.label.setText(formatted_text)
            self.label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)

    def resizeEvent(self, event):
        if self.originalPixmap:
            self.updateImage()
        elif self.originalText:
            self.updateText()
        super().resizeEvent(event)

    def show_context_menu(self, position):
        menu = QMenu()
        copy_image_action = menu.addAction("Copy image")

        action = menu.exec(self.mapToGlobal(position))

        if action == copy_image_action and self.originalPixmap:
            clipboard = QApplication.clipboard()
            mime_data = QMimeData()
            mime_data.setImageData(self.originalPixmap.toImage())
            clipboard.setMimeData(mime_data, QClipboard.Mode.Clipboard)


class LazyItemModel(QStandardItemModel):
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
        if role == Qt.ItemDataRole.DisplayRole and orientation == Qt.Orientation.Horizontal:
            return ["Name", "Size", "Count"][section]
        return None

    def data(self, index, role):
        item = index.internalPointer()
        if role == Qt.ItemDataRole.DisplayRole:
            if index.column() == 0:
                if item.parent == self.root:
                    return '[root]'
                return osp.basename(item.path)
            elif index.column() == 1:
                return format_size(item.size)
            elif index.column() == 2:
                return format_count(item.count)
        elif role == Qt.ItemDataRole.TextAlignmentRole:
            if index.column() in [1, 2]:
                return Qt.AlignmentFlag.AlignRight
        return None

    def canFetchMore(self, index):
        if not index.isValid():
            return False
        return not index.internalPointer().fetched

    def fetchMore(self, index):
        item = index.internalPointer()
        if item == self.root:
            return
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
        subdir_infos = self.file_reader.index.list_subdir_dirinfos(self.path)
        subdir_infos = sorted(subdir_infos, key=lambda x: natural_sort_key(x.path))
        for dinfo in subdir_infos:
            self.children.append(TreeItem(
                self.file_reader, path=dinfo.path, size=dinfo.size_tree, count=dinfo.num_files_tree,
                has_subdirs=dinfo.num_subdirs > 0, parent=self))

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


def natural_sort_key(s):
    """Normal string sort puts '10' before '2'. Natural sort puts '2' before '10'."""
    return [float(t) if t.isdigit() else t for t in re.split('([0-9]+)', s)]


if __name__ == '__main__':
    main()
