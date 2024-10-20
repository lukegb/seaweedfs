package shell

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/encoding/prototext"
)

func init() {
	Commands = append(Commands, &commandFsMetaEdit{})
}

type commandFsMetaEdit struct {
	dirPrefix *string
}

func (c *commandFsMetaEdit) Name() string {
	return "fs.meta.edit"
}

func (c *commandFsMetaEdit) Help() string {
	return `edit single file's metadata

	fs.meta.edit /path/to/single/file
`
}

func (c *commandFsMetaEdit) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsMetaEdit) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if len(args) == 0 {
		fmt.Fprintf(writer, "missing a path to a file\n")
		return nil
	}

	metaEditCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	if err = metaEditCommand.Parse(args); err != nil {
		return nil
	}
	path, parseErr := commandEnv.parseUrl(findInputDirectory(metaEditCommand.Args()))
	if parseErr != nil {
		return parseErr
	}
	filerPath := util.FullPath(path)

	// Fetch entry.
	fmt.Fprintf(writer, "%#v\n", filerPath)
	entry, err := filer_pb.GetEntry(commandEnv, filerPath)
	if err != nil {
		return fmt.Errorf("getting entry data: %w", err)
	}

	// Write entry to temp file.
	f, err := os.CreateTemp("", "seaweed-entry-*.txtpb")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	defer os.Remove(f.Name())

	if _, err := f.Write([]byte(prototext.Format(entry))); err != nil {
		return fmt.Errorf("writing formatted entry data: %w", err)
	}

	// Launch an editor.
	editor := os.Getenv("VISUAL")
	if editor == "" {
		editor = os.Getenv("EDITOR")
	}
	if editor == "" {
		editor = "vim"
	}
	cmd := exec.Command("sh", "-c", fmt.Sprintf("%s %s", editor, f.Name()))
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("editor failed: %w", err)
	}

	// Read edited entry.
	if _, err := f.Seek(0, 0); err != nil {
		return fmt.Errorf("seeking to beginning of file failed: %w", err)
	}
	editedBytes, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("reading edited entry: %w", err)
	}
	if err := prototext.Unmarshal(editedBytes, entry); err != nil {
		return fmt.Errorf("parsing edited entry: %w", err)
	}

	// Write back entry to filer.
	dir, _ := filerPath.DirAndName()
	req := &filer_pb.UpdateEntryRequest{
		Directory: dir,
		Entry:     entry,
	}
	if err := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.UpdateEntry(client, req)
	}); err != nil {
		return fmt.Errorf("updating entry in filer: %w", err)
	}

	return nil
}
