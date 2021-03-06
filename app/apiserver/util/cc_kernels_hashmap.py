# Copyright (c) 2019, MD2K Center of Excellence
# - Nasir Ali <nasir.ali08@gmail.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


from cerebralcortex import Kernel
from data_manager.object.data import ObjectData

class CCKernelHashMap:
    def __init__(self, configs_dir_path):
        self.CC_map = {}
        self.CC_map["default"] = Kernel(configs_dir_path=configs_dir_path, enable_spark=False, study_name="default")
        self.config = self.CC_map["default"].config
        self.config_dir_path = configs_dir_path

    def get_or_create_instance(self, study_name):
        if not study_name in self.CC_map:
            self.CC_map[study_name] = Kernel(configs_dir_path=self.config_dir_path, enable_spark=False, study_name=study_name)
        return self.CC_map[study_name]

class ObjectStorageHashMap:
    def __init__(self, apiserver_config):
        self.obj_storage_map = {}
        self.apiserver_config = apiserver_config
        self.obj_storage_map["default"] = ObjectData(study_name="default", apiserver_config=apiserver_config)

    def get_or_create_instance(self, study_name):
        if not study_name in self.obj_storage_map:
            self.obj_storage_map[study_name] = ObjectData(study_name=study_name, apiserver_config=self.apiserver_config)
        return self.obj_storage_map[study_name]