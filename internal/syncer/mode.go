package syncer

import "os"

// safeFileMode masque les bits de privilège (setuid/setgid/sticky) : ils
// proviennent d'un pair distant et ne doivent pas être appliqués tels quels.
func safeFileMode(bits uint32) os.FileMode {
	return os.FileMode(bits) &^ (os.ModeSetuid | os.ModeSetgid | os.ModeSticky)
}
