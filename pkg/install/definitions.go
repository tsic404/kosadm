package install

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
)

// DefinitionSource supplies the object definition manifests the release ships
// (C-54). Installation applies exactly what the release built, so the served
// definitions cannot drift from the API types they describe.
type DefinitionSource interface {
	Definitions(ctx context.Context) ([]*unstructured.Unstructured, error)
}

// CRDDirectory reads CRD manifests from a directory in an fs.FS. The release
// ships them as generated files; a caller points this at the shipped directory.
type CRDDirectory struct {
	// FS holds the files; nil selects the operating system.
	FS fs.FS
	// Dir is the directory inside FS; empty selects its root.
	Dir string
}

// Definitions reads every CRD manifest under the directory, ordered by name.
func (d CRDDirectory) Definitions(_ context.Context) ([]*unstructured.Unstructured, error) {
	if d.FS == nil {
		return nil, fmt.Errorf("object definitions need a filesystem")
	}
	dir := d.Dir
	if dir == "" {
		dir = "."
	}
	entries, err := fs.ReadDir(d.FS, dir)
	if err != nil {
		return nil, fmt.Errorf("read object definition directory %s: %w", dir, err)
	}
	definitions := make([]*unstructured.Unstructured, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !manifestName(entry.Name()) {
			continue
		}
		path := entry.Name()
		if dir != "." {
			path = dir + "/" + entry.Name()
		}
		content, err := fs.ReadFile(d.FS, path)
		if err != nil {
			return nil, fmt.Errorf("read object definition %s: %w", path, err)
		}
		decoded, err := decodeDefinitions(content)
		if err != nil {
			return nil, fmt.Errorf("read object definition %s: %w", path, err)
		}
		definitions = append(definitions, decoded...)
	}
	sort.SliceStable(definitions, func(left, right int) bool {
		return definitions[left].GetName() < definitions[right].GetName()
	})
	return definitions, nil
}

// manifestName reports whether a directory entry is a YAML manifest.
func manifestName(name string) bool {
	return strings.HasSuffix(name, ".yaml") || strings.HasSuffix(name, ".yml")
}

// decodeDefinitions reads every document of one YAML file that is a CRD.
func decodeDefinitions(content []byte) ([]*unstructured.Unstructured, error) {
	decoder := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(content), 4096)
	definitions := make([]*unstructured.Unstructured, 0, 1)
	for {
		document := map[string]any{}
		err := decoder.Decode(&document)
		if err == io.EOF {
			return definitions, nil
		}
		if err != nil {
			return nil, err
		}
		if len(document) == 0 {
			continue
		}
		object := &unstructured.Unstructured{Object: document}
		if object.GetKind() != "CustomResourceDefinition" {
			continue
		}
		definitions = append(definitions, object)
	}
}
