#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use eframe::egui;

fn main() -> eframe::Result {
    let mut options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([400.0, 300.0])
            .with_title("Gens-Relay P2P"),
        ..Default::default()
    };
    
    // On force la recherche sur TOUS les moteurs (y compris l'ancien DirectX 11 souvent présent dans les VM)
    options.wgpu_options.supported_backends = eframe::wgpu::Backends::all();
    
    eframe::run_native(
        "Gens-Relay",
        options,
        Box::new(|_cc| Ok(Box::<MyApp>::default())),
    )
}

#[derive(Default)]
struct MyApp {}

impl eframe::App for MyApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Bienvenue sur Gens-Relay");
            ui.label("En attente de connexion au réseau P2P...");
            
            ui.add_space(20.0);
            if ui.button("Quitter").clicked() {
                ctx.send_viewport_cmd(egui::ViewportCommand::Close);
            }
        });
    }
}
