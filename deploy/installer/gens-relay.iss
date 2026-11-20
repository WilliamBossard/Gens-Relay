#ifndef AppVersion
  #define AppVersion "2.0.0"
#endif

[Setup]
AppId={{D9B4E210-7A8F-4A2C-9E3B-6D2C1A5F8001}
AppName=Gens-Relay
AppVersion={#AppVersion}
AppPublisher=WilliamBossard
AppPublisherURL=https://github.com/WilliamBossard/Gens-Relay
AppSupportURL=https://github.com/WilliamBossard/Gens-Relay
DefaultDirName={autopf}\Gens-Relay
DefaultGroupName=Gens-Relay
AllowNoIcons=yes
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog
OutputDir=..\..\dist
OutputBaseFilename=gens-relay-setup
SetupIconFile=..\..\desktop\src-tauri\icons\icon.ico
UninstallDisplayIcon={app}\gens-daemon.exe
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
CloseApplications=yes

[Languages]
Name: "french"; MessagesFile: "compiler:Languages\French.isl"
Name: "english"; MessagesFile: "compiler:Default.isl"

[Types]
Name: "full"; Description: "Installation complète (Daemon + Gens-Relay + Gens-Drop)"
Name: "compact"; Description: "Gens-Drop rapide uniquement (Daemon + Gens-Drop)"
Name: "custom"; Description: "Installation personnalisée"; Flags: iscustom

[Components]
Name: "daemon"; Description: "Service Gens-Relay Daemon & CLI gens-drop (Obligatoire)"; Types: full compact custom; Flags: fixed
Name: "desktop"; Description: "Gens-Relay Desktop (Synchronisation complète & QuickDrop)"; Types: full
Name: "drop"; Description: "Gens-Drop (Fenêtre compacte rapide façon Blip)"; Types: full compact

[Tasks]
Name: "autostart"; Description: "Démarrer Gens-Relay automatiquement avec Windows (en arrière-plan)"; GroupDescription: "Options système:"
Name: "contextmenu"; Description: "Ajouter 'Envoyer avec Gens-Drop' au menu contextuel Explorateur"; GroupDescription: "Intégration Windows:"; Components: drop desktop
Name: "desktopicon"; Description: "Créer les raccourcis sur le Bureau"; GroupDescription: "Raccourcis:"; Flags: unchecked

[Files]
Source: "..\..\dist\gens-daemon.exe"; DestDir: "{app}"; Components: daemon; Flags: ignoreversion
Source: "..\..\dist\gens-drop.exe"; DestDir: "{app}"; Components: daemon; Flags: ignoreversion
Source: "..\..\desktop\src-tauri\target\release\gens-relay-desktop.exe"; DestDir: "{app}"; Components: desktop; Flags: ignoreversion
Source: "..\..\drop-desktop\src-tauri\target\release\gens-drop-desktop.exe"; DestDir: "{app}"; Components: drop; Flags: ignoreversion

[Icons]
Name: "{group}\Gens-Relay"; Filename: "{app}\gens-relay-desktop.exe"; Components: desktop
Name: "{group}\Gens-Drop"; Filename: "{app}\gens-drop-desktop.exe"; Components: drop
Name: "{group}\Ajouter ou supprimer des composants"; Filename: "{app}\setup\gens-relay-setup.exe"
Name: "{group}\Désinstaller Gens-Relay"; Filename: "{uninstallexe}"
Name: "{autodesktop}\Gens-Relay"; Filename: "{app}\gens-relay-desktop.exe"; Components: desktop; Tasks: desktopicon
Name: "{autodesktop}\Gens-Drop"; Filename: "{app}\gens-drop-desktop.exe"; Components: drop; Tasks: desktopicon

[Registry]
; Autostart Windows
Root: HKCU; Subkey: "Software\Microsoft\Windows\CurrentVersion\Run"; ValueType: string; ValueName: "Gens-Relay"; ValueData: """{app}\gens-relay-desktop.exe"" --minimized"; Components: desktop; Tasks: autostart; Flags: uninsdeletevalue
Root: HKCU; Subkey: "Software\Microsoft\Windows\CurrentVersion\Run"; ValueType: string; ValueName: "Gens-Drop"; ValueData: """{app}\gens-drop-desktop.exe"" --minimized"; Components: drop and not desktop; Tasks: autostart; Flags: uninsdeletevalue

; Windows Explorer right-click integration (files & folders) -> prefers compact Gens-Drop when installed
Root: HKCU; Subkey: "Software\Classes\*\shell\GensDrop"; ValueType: string; ValueName: ""; ValueData: "Envoyer avec Gens-Drop"; Components: drop; Tasks: contextmenu; Flags: uninsdeletekey
Root: HKCU; Subkey: "Software\Classes\*\shell\GensDrop"; ValueType: string; ValueName: "Icon"; ValueData: """{app}\gens-drop-desktop.exe"""; Components: drop; Tasks: contextmenu
Root: HKCU; Subkey: "Software\Classes\*\shell\GensDrop\command"; ValueType: string; ValueName: ""; ValueData: """{app}\gens-drop-desktop.exe"" ""%1"""; Components: drop; Tasks: contextmenu

Root: HKCU; Subkey: "Software\Classes\Directory\shell\GensDrop"; ValueType: string; ValueName: ""; ValueData: "Envoyer avec Gens-Drop"; Components: drop; Tasks: contextmenu; Flags: uninsdeletekey
Root: HKCU; Subkey: "Software\Classes\Directory\shell\GensDrop"; ValueType: string; ValueName: "Icon"; ValueData: """{app}\gens-drop-desktop.exe"""; Components: drop; Tasks: contextmenu
Root: HKCU; Subkey: "Software\Classes\Directory\shell\GensDrop\command"; ValueType: string; ValueName: ""; ValueData: """{app}\gens-drop-desktop.exe"" ""%1"""; Components: drop; Tasks: contextmenu

Root: HKCU; Subkey: "Software\Classes\*\shell\GensDrop"; ValueType: string; ValueName: ""; ValueData: "Envoyer avec Gens-Relay"; Components: desktop and not drop; Tasks: contextmenu; Flags: uninsdeletekey
Root: HKCU; Subkey: "Software\Classes\*\shell\GensDrop"; ValueType: string; ValueName: "Icon"; ValueData: """{app}\gens-relay-desktop.exe"""; Components: desktop and not drop; Tasks: contextmenu
Root: HKCU; Subkey: "Software\Classes\*\shell\GensDrop\command"; ValueType: string; ValueName: ""; ValueData: """{app}\gens-relay-desktop.exe"" ""%1"""; Components: desktop and not drop; Tasks: contextmenu

Root: HKCU; Subkey: "Software\Classes\Directory\shell\GensDrop"; ValueType: string; ValueName: ""; ValueData: "Envoyer avec Gens-Relay"; Components: desktop and not drop; Tasks: contextmenu; Flags: uninsdeletekey
Root: HKCU; Subkey: "Software\Classes\Directory\shell\GensDrop"; ValueType: string; ValueName: "Icon"; ValueData: """{app}\gens-relay-desktop.exe"""; Components: desktop and not drop; Tasks: contextmenu
Root: HKCU; Subkey: "Software\Classes\Directory\shell\GensDrop\command"; ValueType: string; ValueName: ""; ValueData: """{app}\gens-relay-desktop.exe"" ""%1"""; Components: desktop and not drop; Tasks: contextmenu

[UninstallDelete]
Type: filesandordirs; Name: "{app}\setup"

[Run]
Filename: "{app}\gens-relay-desktop.exe"; Description: "Lancer Gens-Relay maintenant"; Components: desktop; Flags: nowait postinstall skipifsilent
Filename: "{app}\gens-drop-desktop.exe"; Description: "Lancer Gens-Drop maintenant"; Components: drop; Flags: nowait postinstall skipifsilent unchecked

[Code]
procedure CurStepChanged(CurStep: TSetupStep);
var
  SetupDir: string;
begin
  if CurStep = ssPostInstall then
  begin
    SetupDir := ExpandConstant('{app}\setup');
    ForceDirectories(SetupDir);
    FileCopy(ExpandConstant('{srcexe}'), SetupDir + '\gens-relay-setup.exe', False);
  end;
end;
