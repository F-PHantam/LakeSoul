# LakeSoul's Supports for Python and Machine Learning

LakeSoul implements interfaces for PyTorch/PyArrow/HuggingFace/Ray, allowing users to retrieve datasets from LakeSoul
tables through the interfaces. Distributed reading is supported for both PyTorch and Ray. LakeSoul for Python has now
released 1.0 Beta.

## Install

### Download LakeSoul wheel file

For users of Python 3.8, Python 3.9, and Python 3.10, we have prepared different wheel files for each version. Please
download the appropriate one based on your requirements. We will publish official package to pypi.org in near future.

The Python package currently only supports Linux x86_64 systems. The Python package is built based on the manylinux_2_28 image and supports CentOS 8, Debian 10, Ubuntu 18.10 and above OS versions (for specific compatibility, see [Distro compatibility](https://github.com/mayeut/pep600_compliance?tab=readme-ov-file#distro-compatibility)). If you need to run it on an earlier OS version, it is recommended to use Docker container.

* For Python 3.8
  users: [lakesoul-1.0.0b2-cp38-cp38-manylinux_2_28_x86_64.whl](https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/python/v1.0/lakesoul-1.0.0b2-cp38-cp38-manylinux_2_28_x86_64.whl)
* For Python 3.9
  users: [lakesoul-1.0.0b2-cp39-cp39-manylinux_2_28_x86_64.whl](https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/python/v1.0/lakesoul-1.0.0b2-cp39-cp39-manylinux_2_28_x86_64.whl)
* For Python 3.10
  users: [lakesoul-1.0.0b2-cp310-cp310-manylinux_2_28_x86_64.whl](https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/python/v1.0/lakesoul-1.0.0b2-cp310-cp310-manylinux_2_28_x86_64.whl)

Assuming we are using Python 3.8, we can down load the wheel file as below

```bash
wget https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/python/v1.0/lakesoul-1.0.0b2-cp38-cp38-manylinux_2_28_x86_64.whl
```

### Install python virtual enviroment

We have provide several AI training examples using LakeSoul as data source. Follow the below instructions to setup a
testing environment.

```bash 
# change python version if needed
conda create -n lakesoul_test python=3.8
conda activate lakesoul_test
git clone https://github.com/lakesoul-io/LakeSoul.git
cd LakeSoul/python/examples
# replace ${PWD} with your wheel file directory in requirements.txt
pip install -r requirements.txt
```

### LakeSoul environment setup
When using, you need to refer to the method in the [LakeSoul Quick Environment](../01-Getting%20Started/01-setup-local-env.md) document to setup the LakeSoul environment, and configure the connection information of the LakeSoul metadata library through the environment variables: `LAKESOUL_PG_URL`, `LAKESOUL_PG_USERNAME`, `LAKESOUL_PG_PASSWORD`. If you use docker compose to start the local test environment as described in the document, then these environment variables are:
```bash
export LAKESOUL_PG_URL=jdbc:postgresql://localhost:5432/lakesoul_test?stringtype=unspecified
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test
```

## PyTorch API Usage

LakeSoul implements interfaces for PyTorch/HuggingFace, which allows users to directly export data from LakeSoul tables
into HuggingFace datasets.

API for reading tables:

```python
import datasets
import lakesoul.huggingface

dataset = datasets.IterableDataset.from_lakesoul("lakesoul_table", partitions={'split': 'train'})
```

You can create a PyTorch/HuggingFace dataset for training. The distributed training environment is automatically aware
and no additional parameters are required when initializing the dataset.

Below is an example code that exports the feature-transformed [Titanic](https://www.kaggle.com/competitions/titanic)
dataset from LakeSoul and then trains and validates a DNN model using the dataset.

```python
import argparse
import pandas as pd
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.nn.init as init

import datasets
import lakesoul.huggingface

from torch.autograd import Variable

# hyper parameters
SEED = 0
torch.manual_seed(SEED)
torch.cuda.manual_seed(SEED)
batch_size = 50
num_epochs = 50
learning_rate = 0.01
weight_decay = 0.005

# label and feature columns
label_column = 'label'
feature_columns = 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f22,f23,f24,f25,f26'.split(
    ',')


class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.bn = nn.BatchNorm1d(26)
        self.fc1 = nn.Linear(26, 256, bias=True)
        self.fc2 = nn.Linear(256, 2, bias=True)
        self._initialize_weights()

    def forward(self, x):
        x = self.bn(x)
        x = self.fc1(x)
        x = F.relu(x)
        x = self.fc2(x)
        x = torch.sigmoid(x)
        return x

    def _initialize_weights(self):
        for m in self.modules():
            if isinstance(m, nn.Linear):
                init.xavier_uniform_(m.weight)
                if m.bias is not None:
                    init.constant_(m.bias, 0)


def batchify(dataset, batch_size):
    X_train = []
    y_train = []
    for i, item in enumerate(dataset):
        feature_list = [item[feature] for feature in feature_columns]
        X_train.append(feature_list)
        y_train.append(int(item[label_column]))
        if len(y_train) == batch_size:
            yield X_train, y_train
            X_train = []
            y_train = []
    # Handle the remaining records that don't fill up a full batch
    if len(y_train) > 0:
        yield X_train, y_train


def train_model(net, datasource, num_epochs, batch_size, learning_rate):
    dataset = datasets.IterableDataset.from_lakesoul(datasource, partitions={'split': 'train'})

    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.AdamW(net.parameters(), lr=learning_rate, weight_decay=weight_decay)

    for epoch in range(num_epochs):
        if epoch % 5 == 0:
            print('Epoch {}'.format(epoch + 1))
        for X_train, y_train in batchify(dataset, batch_size):
            x_var = Variable(torch.FloatTensor(X_train))
            y_var = Variable(torch.LongTensor(y_train))
            optimizer.zero_grad()
            ypred_var = net(x_var)
            loss = criterion(ypred_var, y_var)
            loss.backward()
            optimizer.step()


def evaluate_model(net, datasource, batch_size):
    dataset = datasets.IterableDataset.from_lakesoul(datasource, partitions={'split': 'val'})
    num_samples = 0
    num_correct = 0

    for X_val, y_val in batchify(dataset, batch_size):
        batch_size = len(y_val)
        test_var = Variable(torch.FloatTensor(X_val))
        with torch.no_grad():
            result = net(test_var)
        values, labels = torch.max(result, 1)
        num_right = np.sum(labels.data.numpy() == y_val)
        num_samples += batch_size
        num_correct += num_right

    accuracy = num_correct / num_samples
    print('Accuracy {:.2f}'.format(accuracy))


def main(table):
    net = Net()
    train_model(net, table, batch_size, num_epochs, learning_rate)
    evaluate_model(net, table, batch_size)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', type=str, default='titanic_trans', help='lakesoul table name')
    args = parser.parse_args()

    main(args.table)

```

More Examples at  [LakeSoul/python/examples](https://github.com/lakesoul-io/LakeSoul/tree/main/python/examples)

## Ray DataSource

LakeSoul implements Ray's [Datasource](https://docs.ray.io/en/latest/data/api/doc/ray.data.Datasource.html). The
following is an example of calling code:

```python
import ray.data
import lakesoul.ray

ds = ray.data.read_lakesoul("table_name", partitions={'split': 'train'})
```

## PyArrow/Pandas Reads LakeSoul Table

LakeSoul can support single-machine reading of data and use PyArrow and Pandas for calculations. LakeSoul returns
PyArrow's [Dataset](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html) object when reading,
supporting iterative access. Example:

```python
from lakesoul.arrow import lakesoul_dataset

ds = lakesoul_dataset("table_name", partitions={'split': 'train'})

# iterate batches in dataset
# this will not load entire table to memory
for batch in ds.to_batches():
    ...

# convert to pandas table
# this will load entire table into memory
df = ds.to_table().to_pandas()
```