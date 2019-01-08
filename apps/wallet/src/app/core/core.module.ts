import { NgModule } from '@angular/core';
import { HttpClientModule } from '@angular/common/http';
import { AppbarModule, SidenavModule } from './components';

import {
  PublicNode,
  BridgeService,
  EncoderService,
  WalletService,
  AuthService,
  addresValidatorProvider,
  Sidenav
} from './services';

@NgModule({
  declarations: [],
  imports: [HttpClientModule],
  exports: [AppbarModule, SidenavModule],
  providers: [
    PublicNode.provider,
    BridgeService.provider,
    EncoderService,
    WalletService.provider,
    AuthService.provider,
    addresValidatorProvider,
    Sidenav.provider
  ]
})
export class CoreModule {}
