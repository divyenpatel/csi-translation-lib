/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package plugins

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// VSphereCSIDriverName is the name of the CSI driver for vSphere Volume
	VSphereDriverName = "csi.vsphere.vmware.com"
	// VSphereInTreePluginName is the name of the intree plugin for vSphere Volume
	VSphereInTreePluginName = "kubernetes.io/vsphere-volume"
)

var _ InTreePlugin = &vSphereCSITranslator{}

// vSphereCSITranslator handles translation of PV spec from In-tree vSphere Volume to vSphere CSI
type vSphereCSITranslator struct{}

// NewvSphereCSITranslator returns a new instance of vSphereCSITranslator
func NewvSphereCSITranslator() InTreePlugin {
	return &vSphereCSITranslator{}
}

// TranslateInTreeStorageClassToCSI translates InTree vSphere storage class parameters to CSI storage class
func (t *vSphereCSITranslator) TranslateInTreeStorageClassToCSI(sc *storage.StorageClass) (*storage.StorageClass, error) {
	var params = map[string]string{}
	for k, v := range sc.Parameters {
		switch strings.ToLower(k) {
		case fsTypeKey:
			params[csiFsTypeKey] = v
		default:
			params[k] = v
		}
	}
	sc.Parameters = params
	return sc, nil
}

// TranslateInTreeInlineVolumeToCSI takes a Volume with VsphereVolume set from in-tree
// and converts the VsphereVolume source to a CSIPersistentVolumeSource
func (t *vSphereCSITranslator) TranslateInTreeInlineVolumeToCSI(volume *v1.Volume) (*v1.PersistentVolume, error) {
	if volume == nil || volume.VsphereVolume == nil {
		return nil, fmt.Errorf("volume is nil or VsphereVolume not defined on volume")
	}
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			// Must be unique per disk as it is used as the unique part of the
			// staging path
			Name: fmt.Sprintf("%s-%s", VSphereDriverName, volume.VsphereVolume.VolumePath),
		},

		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       VSphereDriverName,
					VolumeHandle: volume.VsphereVolume.VolumePath,
					FSType:       volume.VsphereVolume.FSType,
					VolumeAttributes: map[string]string{
						"storagepolicyname": volume.VsphereVolume.StoragePolicyName,
					},
				},
			},
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
		},
	}
	return pv, nil
}

// TranslateInTreePVToCSI takes a PV with VsphereVolume set from in-tree
// and converts the VsphereVolume source to a CSIPersistentVolumeSource
func (t *vSphereCSITranslator) TranslateInTreePVToCSI(pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	if pv == nil || pv.Spec.VsphereVolume == nil {
		return nil, fmt.Errorf("pv is nil or VsphereVolume not defined on pv")
	}
	csiSource := &v1.CSIPersistentVolumeSource{
		Driver:       VSphereDriverName,
		VolumeHandle: pv.Spec.VsphereVolume.VolumePath,
		FSType:       pv.Spec.VsphereVolume.FSType,
		VolumeAttributes: map[string]string{
			"storagepolicyname": pv.Spec.VsphereVolume.StoragePolicyName,
		},
	}
	pv.Spec.VsphereVolume = nil
	pv.Spec.CSI = csiSource
	return pv, nil
}

// TranslateCSIPVToInTree takes a PV with CSIPersistentVolumeSource set and
// translates the vSphere CSI source to a vSphereVolume source.
func (t *vSphereCSITranslator) TranslateCSIPVToInTree(pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	return nil, fmt.Errorf("VsphereVolume does not support TranslateCSIPVToInTree")
}

// CanSupport tests whether the plugin supports a given persistent volume
// specification from the API.  The spec pointer should be considered
// const.
func (t *vSphereCSITranslator) CanSupport(pv *v1.PersistentVolume) bool {
	return pv != nil && pv.Spec.VsphereVolume != nil
}

// CanSupportInline tests whether the plugin supports a given inline volume
// specification from the API.  The spec pointer should be considered
// const.
func (t *vSphereCSITranslator) CanSupportInline(volume *v1.Volume) bool {
	return volume != nil && volume.VsphereVolume != nil
}

// GetInTreePluginName returns the name of the in tree plugin driver
func (t *vSphereCSITranslator) GetInTreePluginName() string {
	return VSphereInTreePluginName
}

// GetCSIPluginName returns the name of the CSI plugin
func (t *vSphereCSITranslator) GetCSIPluginName() string {
	return VSphereDriverName
}

func (t *vSphereCSITranslator) RepairVolumeHandle(volumeHandle, nodeID string) (string, error) {
	return volumeHandle, nil
}

